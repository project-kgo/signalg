package signalg

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kanengo/ku/mapx"
	"github.com/kanengo/ku/poolx/slicepool"
)

const registryShardNum = 64

type connectionSet = mapx.ShardMap[string, *Connection]
type groupSet = mapx.ShardMap[string, struct{}]

var connectionSlicePool = &slicepool.Pool[*Connection]{}

type pooledConnections struct {
	connections []*Connection
}

func (p pooledConnections) release() {
	putConnectionSlice(p.connections)
}

type connectionRegistry struct {
	connections    *mapx.ShardMap[string, *connectionNode]
	userConnIndex  *mapx.ShardMap[string, *connectionSet]
	groupConnIndex *mapx.ShardMap[string, *connectionSet]
	connGroupIndex *mapx.ShardMap[string, *groupSet]

	lastSeenLists  [registryShardNum]connectionList
	connLocks      stripedMutexes
	indexInitLocks stripedMutexes
}

type connectionNode struct {
	conn     *Connection
	connKey  string
	lastSeen time.Time
	prev     *connectionNode
	next     *connectionNode
}

type connectionList struct {
	head *connectionNode
	tail *connectionNode
}

func newConnectionRegistry() *connectionRegistry {
	return &connectionRegistry{
		connections:    newConnectionMap(),
		userConnIndex:  newConnectionSetIndex(),
		groupConnIndex: newConnectionSetIndex(),
		connGroupIndex: newGroupSetIndex(),
	}
}

func (r *connectionRegistry) add(conn *Connection) {
	if conn == nil {
		return
	}
	connKey := connectionKey(conn)
	unlockConn := r.connLocks.lock(connKey)
	defer unlockConn()

	node := &connectionNode{
		conn:     conn,
		connKey:  connKey,
		lastSeen: time.Now(),
	}
	r.connections.Set(connKey, node)
	r.listFor(connKey).pushBack(node)
	if conn.UserID != "" {
		r.connectionSetFor(r.userConnIndex, conn.UserID).Set(connKey, conn)
	}
}

func (r *connectionRegistry) remove(conn *Connection) bool {
	if conn == nil {
		return false
	}
	connKey := connectionKey(conn)
	unlockConn := r.connLocks.lock(connKey)
	defer unlockConn()

	node, ok := r.connections.Get(connKey)
	if !ok || node.conn != conn {
		return false
	}
	r.listFor(connKey).remove(node)
	r.connections.Delete(connKey)
	if node.conn.UserID != "" {
		r.removeConnectionFromIndex(r.userConnIndex, node.conn.UserID, connKey)
	}
	if connGroups, ok := r.connGroupIndex.Get(connKey); ok {
		connGroups.Range(func(group string, _ struct{}) bool {
			r.removeConnectionFromIndex(r.groupConnIndex, group, connKey)
			return true
		})
	}
	r.connGroupIndex.Delete(connKey)
	return true
}

func (r *connectionRegistry) allConnections() []*Connection {
	if r.connections.Len() == 0 {
		return nil
	}
	connections := make([]*Connection, 0, r.connections.Len())
	r.connections.Range(func(_ string, node *connectionNode) bool {
		if node != nil && node.conn != nil {
			connections = append(connections, node.conn)
		}
		return true
	})
	return connections
}

func (r *connectionRegistry) allConnectionsPooled() pooledConnections {
	if r.connections.Len() == 0 {
		return pooledConnections{}
	}
	connections := getConnectionSlice(r.connections.Len())
	r.connections.Range(func(_ string, node *connectionNode) bool {
		if node != nil && node.conn != nil {
			connections = append(connections, node.conn)
		}
		return true
	})
	return pooledConnections{connections: connections}
}

func (r *connectionRegistry) userOnline(userID string) int {
	userConnections, ok := r.userConnIndex.Get(userID)
	if !ok {
		return 0
	}
	return userConnections.Len()
}

func (r *connectionRegistry) userConnections(userID string) []*Connection {
	userConnections, ok := r.userConnIndex.Get(userID)
	if !ok {
		return nil
	}
	return copyConnectionSet(userConnections)
}

func (r *connectionRegistry) connectionSnapshot(connectionIDs []string) []*Connection {
	if len(connectionIDs) == 0 {
		return nil
	}
	if len(connectionIDs) == 1 {
		connectionID := connectionIDs[0]
		if connectionID == "" {
			return nil
		}
		node, ok := r.connections.Get(connectionID)
		if !ok || node == nil || node.conn == nil {
			return nil
		}
		return []*Connection{node.conn}
	}

	connections := make([]*Connection, 0, len(connectionIDs))
	seen := make(map[string]struct{}, len(connectionIDs))
	for _, connectionID := range connectionIDs {
		if connectionID == "" {
			continue
		}
		if _, ok := seen[connectionID]; ok {
			continue
		}
		seen[connectionID] = struct{}{}
		node, ok := r.connections.Get(connectionID)
		if ok && node != nil && node.conn != nil {
			connections = append(connections, node.conn)
		}
	}
	return connections
}

func (r *connectionRegistry) connectionSnapshotPooled(connectionIDs []string) pooledConnections {
	if len(connectionIDs) == 0 {
		return pooledConnections{}
	}
	if len(connectionIDs) == 1 {
		connectionID := connectionIDs[0]
		if connectionID == "" {
			return pooledConnections{}
		}
		node, ok := r.connections.Get(connectionID)
		if !ok || node == nil || node.conn == nil {
			return pooledConnections{}
		}
		connections := getConnectionSlice(1)
		connections = append(connections, node.conn)
		return pooledConnections{connections: connections}
	}

	connections := getConnectionSlice(len(connectionIDs))
	seen := make(map[string]struct{}, len(connectionIDs))
	for _, connectionID := range connectionIDs {
		if connectionID == "" {
			continue
		}
		if _, ok := seen[connectionID]; ok {
			continue
		}
		seen[connectionID] = struct{}{}
		node, ok := r.connections.Get(connectionID)
		if ok && node != nil && node.conn != nil {
			connections = append(connections, node.conn)
		}
	}
	return pooledConnections{connections: connections}
}

func (r *connectionRegistry) userSnapshot(userIDs []string) []*Connection {
	if len(userIDs) == 0 {
		return nil
	}
	if len(userIDs) == 1 {
		userID := userIDs[0]
		if userID == "" {
			return nil
		}
		return r.userConnections(userID)
	}

	total := 0
	for _, userID := range userIDs {
		if userID == "" {
			continue
		}
		if userConnections, ok := r.userConnIndex.Get(userID); ok {
			total += userConnections.Len()
		}
	}
	if total == 0 {
		return nil
	}

	connections := make([]*Connection, 0, total)
	seen := make(map[string]struct{}, total)
	for _, userID := range userIDs {
		if userID == "" {
			continue
		}
		userConnections, ok := r.userConnIndex.Get(userID)
		if !ok {
			continue
		}
		userConnections.Range(func(connKey string, conn *Connection) bool {
			if _, ok := seen[connKey]; ok {
				return true
			}
			seen[connKey] = struct{}{}
			if conn != nil {
				connections = append(connections, conn)
			}
			return true
		})
	}
	return connections
}

func (r *connectionRegistry) userSnapshotPooled(userIDs []string) pooledConnections {
	if len(userIDs) == 0 {
		return pooledConnections{}
	}
	if len(userIDs) == 1 {
		userID := userIDs[0]
		if userID == "" {
			return pooledConnections{}
		}
		userConnections, ok := r.userConnIndex.Get(userID)
		if !ok {
			return pooledConnections{}
		}
		return copyConnectionSetPooled(userConnections)
	}

	total := 0
	for _, userID := range userIDs {
		if userID == "" {
			continue
		}
		if userConnections, ok := r.userConnIndex.Get(userID); ok {
			total += userConnections.Len()
		}
	}
	if total == 0 {
		return pooledConnections{}
	}

	connections := getConnectionSlice(total)
	seen := make(map[string]struct{}, total)
	for _, userID := range userIDs {
		if userID == "" {
			continue
		}
		userConnections, ok := r.userConnIndex.Get(userID)
		if !ok {
			continue
		}
		userConnections.Range(func(connKey string, conn *Connection) bool {
			if _, ok := seen[connKey]; ok {
				return true
			}
			seen[connKey] = struct{}{}
			if conn != nil {
				connections = append(connections, conn)
			}
			return true
		})
	}
	return pooledConnections{connections: connections}
}

func (r *connectionRegistry) addToGroup(conn *Connection, group string) error {
	group = normalizeGroup(group)
	if group == "" {
		return ErrInvalidGroup
	}
	if conn == nil {
		return ErrConnectionNotFound
	}

	connKey := connectionKey(conn)
	unlockConn := r.connLocks.lock(connKey)
	defer unlockConn()

	node, ok := r.connections.Get(connKey)
	if !ok || node.conn != conn {
		return ErrConnectionNotFound
	}
	r.connectionSetFor(r.groupConnIndex, group).Set(connKey, conn)
	r.groupSetFor(connKey).Set(group, struct{}{})
	return nil
}

func (r *connectionRegistry) removeFromGroup(conn *Connection, group string) error {
	group = normalizeGroup(group)
	if group == "" {
		return ErrInvalidGroup
	}
	if conn == nil {
		return ErrConnectionNotFound
	}

	connKey := connectionKey(conn)
	unlockConn := r.connLocks.lock(connKey)
	defer unlockConn()

	node, ok := r.connections.Get(connKey)
	if !ok || node.conn != conn {
		return ErrConnectionNotFound
	}
	r.removeConnectionFromIndex(r.groupConnIndex, group, connKey)
	r.removeGroupFromConnection(connKey, group)
	return nil
}

func (r *connectionRegistry) removeFromAllGroups(conn *Connection) {
	if conn == nil {
		return
	}
	connKey := connectionKey(conn)
	unlockConn := r.connLocks.lock(connKey)
	defer unlockConn()

	connGroups, ok := r.connGroupIndex.Get(connKey)
	if !ok {
		return
	}
	connGroups.Range(func(group string, _ struct{}) bool {
		r.removeConnectionFromIndex(r.groupConnIndex, group, connKey)
		return true
	})
	r.connGroupIndex.Delete(connKey)
}

func (r *connectionRegistry) touch(conn *Connection) {
	if conn == nil {
		return
	}
	connKey := connectionKey(conn)
	unlockConn := r.connLocks.lock(connKey)
	defer unlockConn()

	node, ok := r.connections.Get(connKey)
	if !ok || node.conn != conn {
		return
	}
	node.lastSeen = time.Now()
	list := r.listFor(connKey)
	list.moveToBack(node)
}

func (r *connectionRegistry) expired(now time.Time, timeout time.Duration) []*Connection {
	if timeout <= 0 {
		return nil
	}
	var expired []*Connection
	for shard := 0; shard < registryShardNum; shard++ {
		expired = r.appendExpiredFromShard(expired, shard, now, timeout)
	}
	return expired
}

func (r *connectionRegistry) nextExpiration(now time.Time, timeout time.Duration) (time.Duration, bool) {
	if timeout <= 0 {
		return 0, false
	}

	var next time.Time
	found := false
	for shard := 0; shard < registryShardNum; shard++ {
		unlock := r.connLocks.lockIndex(shard)
		head := r.lastSeenLists[shard].head
		if head != nil {
			deadline := head.lastSeen.Add(timeout)
			if !found || deadline.Before(next) {
				next = deadline
				found = true
			}
		}
		unlock()
	}
	if !found {
		return 0, false
	}
	if !next.After(now) {
		return 0, true
	}
	return next.Sub(now), true
}

func (r *connectionRegistry) appendExpiredFromShard(expired []*Connection, shard int, now time.Time, timeout time.Duration) []*Connection {
	unlock := r.connLocks.lockIndex(shard)
	defer unlock()

	list := &r.lastSeenLists[shard]
	for {
		node := list.head
		if node == nil || now.Sub(node.lastSeen) <= timeout {
			return expired
		}
		list.remove(node)
		r.connections.Delete(node.connKey)
		if node.conn.UserID != "" {
			r.removeConnectionFromIndex(r.userConnIndex, node.conn.UserID, node.connKey)
		}
		if connGroups, ok := r.connGroupIndex.Get(node.connKey); ok {
			connGroups.Range(func(group string, _ struct{}) bool {
				r.removeConnectionFromIndex(r.groupConnIndex, group, node.connKey)
				return true
			})
		}
		r.connGroupIndex.Delete(node.connKey)
		expired = append(expired, node.conn)
	}
}

func (r *connectionRegistry) groupOnline(group string) int {
	groupConnections, ok := r.groupConnIndex.Get(group)
	if !ok {
		return 0
	}
	return groupConnections.Len()
}

func (r *connectionRegistry) groupConnections(group string) []*Connection {
	groupConnections, ok := r.groupConnIndex.Get(group)
	if !ok {
		return nil
	}
	return copyConnectionSet(groupConnections)
}

func (r *connectionRegistry) groupConnectionsPooled(group string) pooledConnections {
	groupConnections, ok := r.groupConnIndex.Get(group)
	if !ok {
		return pooledConnections{}
	}
	return copyConnectionSetPooled(groupConnections)
}

func (r *connectionRegistry) connectionSetFor(index *mapx.ShardMap[string, *connectionSet], indexKey string) *connectionSet {
	if connections, ok := index.Get(indexKey); ok {
		return connections
	}

	unlock := r.indexInitLocks.lock(indexKey)
	defer unlock()

	connections, ok := index.Get(indexKey)
	if !ok {
		connections = newConnectionSet()
		index.Set(indexKey, connections)
	}
	return connections
}

func (r *connectionRegistry) removeConnectionFromIndex(index *mapx.ShardMap[string, *connectionSet], indexKey, connKey string) {
	connections, ok := index.Get(indexKey)
	if !ok {
		return
	}
	connections.Delete(connKey)
}

func (r *connectionRegistry) groupSetFor(connKey string) *groupSet {
	connGroups, ok := r.connGroupIndex.Get(connKey)
	if !ok {
		connGroups = newGroupSet()
		r.connGroupIndex.Set(connKey, connGroups)
	}
	return connGroups
}

func (r *connectionRegistry) removeGroupFromConnection(connKey, group string) {
	connGroups, ok := r.connGroupIndex.Get(connKey)
	if !ok {
		return
	}
	connGroups.Delete(group)
	if connGroups.Len() == 0 {
		r.connGroupIndex.Delete(connKey)
	}
}

func copyConnectionSet(set *connectionSet) []*Connection {
	if set == nil {
		return nil
	}
	connections := make([]*Connection, 0, set.Len())
	set.Range(func(_ string, conn *Connection) bool {
		if conn != nil {
			connections = append(connections, conn)
		}
		return true
	})
	return connections
}

func copyConnectionSetPooled(set *connectionSet) pooledConnections {
	if set == nil {
		return pooledConnections{}
	}
	connections := getConnectionSlice(set.Len())
	set.Range(func(_ string, conn *Connection) bool {
		if conn != nil {
			connections = append(connections, conn)
		}
		return true
	})
	return pooledConnections{connections: connections}
}

func getConnectionSlice(size int) []*Connection {
	if size <= 0 {
		return nil
	}
	return connectionSlicePool.Get(size)[:0]
}

func putConnectionSlice(connections []*Connection) {
	if cap(connections) == 0 {
		return
	}
	backing := connections[:cap(connections)]
	clear(backing)
	connectionSlicePool.Put(backing[:0])
}

func newConnectionMap() *mapx.ShardMap[string, *connectionNode] {
	return mapx.NewShardMap[string, *connectionNode](mapx.ShardMapOptions[string, *connectionNode]{
		ShardNum: registryShardNum,
	})
}

func newConnectionSetIndex() *mapx.ShardMap[string, *connectionSet] {
	return mapx.NewShardMap[string, *connectionSet](mapx.ShardMapOptions[string, *connectionSet]{
		ShardNum: registryShardNum,
	})
}

func newGroupSetIndex() *mapx.ShardMap[string, *groupSet] {
	return mapx.NewShardMap[string, *groupSet](mapx.ShardMapOptions[string, *groupSet]{
		ShardNum: registryShardNum,
	})
}

func newConnectionSet() *connectionSet {
	return mapx.NewShardMap[string, *Connection](mapx.ShardMapOptions[string, *Connection]{
		ShardNum: registryShardNum,
	})
}

func newGroupSet() *groupSet {
	return mapx.NewShardMap[string, struct{}](mapx.ShardMapOptions[string, struct{}]{
		ShardNum: registryShardNum,
	})
}

func connectionKey(conn *Connection) string {
	if conn.ID == "" {
		return fmt.Sprintf("%p", conn)
	}
	return conn.ID
}

func (r *connectionRegistry) listFor(connKey string) *connectionList {
	return &r.lastSeenLists[lockShard(connKey)]
}

func (l *connectionList) pushBack(node *connectionNode) {
	if node == nil {
		return
	}
	node.prev = l.tail
	node.next = nil
	if l.tail != nil {
		l.tail.next = node
	} else {
		l.head = node
	}
	l.tail = node
}

func (l *connectionList) remove(node *connectionNode) {
	if node == nil {
		return
	}
	if node.prev != nil {
		node.prev.next = node.next
	} else if l.head == node {
		l.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else if l.tail == node {
		l.tail = node.prev
	}
	node.prev = nil
	node.next = nil
}

func (l *connectionList) moveToBack(node *connectionNode) {
	if node == nil || l.tail == node {
		return
	}
	l.remove(node)
	l.pushBack(node)
}

type stripedMutexes struct {
	locks [registryShardNum]sync.Mutex
}

func (s *stripedMutexes) lock(key string) func() {
	mutex := &s.locks[lockShard(key)]
	mutex.Lock()
	return mutex.Unlock
}

func (s *stripedMutexes) lockIndex(index int) func() {
	mutex := &s.locks[index]
	mutex.Lock()
	return mutex.Unlock
}

func lockShard(key string) int {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime64
	}
	return int(hash % registryShardNum)
}

func normalizeGroup(group string) string {
	return strings.TrimSpace(group)
}
