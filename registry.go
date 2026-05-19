package signalg

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kanengo/ku/mapx"
	"github.com/kanengo/ku/poolx/slicepool"
)

const registryShardNum = 64

type connectionSet = mapx.ShardMap[string, *Connection]

var connectionSlicePool = &slicepool.Pool[*Connection]{}

type pooledConnections struct {
	connections []*Connection
}

func (p pooledConnections) release() {
	putConnectionSlice(p.connections)
}

type registryEntry struct {
	conn     *Connection
	lastSeen atomic.Int64
	groups   map[string]struct{} // protected by connLocks
}

type connectionRegistry struct {
	connections    *mapx.ShardMap[string, *registryEntry]
	userConnIndex  *mapx.ShardMap[string, *connectionSet]
	groupConnIndex *mapx.ShardMap[string, *connectionSet]

	connLocks      stripedMutexes
	indexInitLocks stripedMutexes
}

func newConnectionRegistry() *connectionRegistry {
	return &connectionRegistry{
		connections:    newConnectionMap(),
		userConnIndex:  newConnectionSetIndex(),
		groupConnIndex: newConnectionSetIndex(),
	}
}

func (r *connectionRegistry) add(conn *Connection) {
	if conn == nil {
		return
	}
	connKey := connectionKey(conn)
	unlockConn := r.connLocks.lock(connKey)
	defer unlockConn()

	entry := &registryEntry{
		conn:   conn,
		groups: make(map[string]struct{}),
	}
	entry.lastSeen.Store(time.Now().UnixNano())
	r.connections.Set(connKey, entry)
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

	entry, ok := r.connections.Get(connKey)
	if !ok || entry.conn != conn {
		return false
	}
	r.connections.Delete(connKey)
	if entry.conn.UserID != "" {
		r.removeConnectionFromIndex(r.userConnIndex, entry.conn.UserID, connKey)
	}
	for group := range entry.groups {
		r.removeConnectionFromIndex(r.groupConnIndex, group, connKey)
	}
	return true
}

func (r *connectionRegistry) allConnections() []*Connection {
	if r.connections.Len() == 0 {
		return nil
	}
	connections := make([]*Connection, 0, r.connections.Len())
	r.connections.Range(func(_ string, entry *registryEntry) bool {
		if entry != nil && entry.conn != nil {
			connections = append(connections, entry.conn)
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
	r.connections.Range(func(_ string, entry *registryEntry) bool {
		if entry != nil && entry.conn != nil {
			connections = append(connections, entry.conn)
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
		entry, ok := r.connections.Get(connectionID)
		if !ok || entry == nil || entry.conn == nil {
			return nil
		}
		return []*Connection{entry.conn}
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
		entry, ok := r.connections.Get(connectionID)
		if ok && entry != nil && entry.conn != nil {
			connections = append(connections, entry.conn)
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
		entry, ok := r.connections.Get(connectionID)
		if !ok || entry == nil || entry.conn == nil {
			return pooledConnections{}
		}
		connections := getConnectionSlice(1)
		connections = append(connections, entry.conn)
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
		entry, ok := r.connections.Get(connectionID)
		if ok && entry != nil && entry.conn != nil {
			connections = append(connections, entry.conn)
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

	entry, ok := r.connections.Get(connKey)
	if !ok || entry.conn != conn {
		return ErrConnectionNotFound
	}
	entry.groups[group] = struct{}{}
	r.connectionSetFor(r.groupConnIndex, group).Set(connKey, conn)
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

	entry, ok := r.connections.Get(connKey)
	if !ok || entry.conn != conn {
		return ErrConnectionNotFound
	}
	delete(entry.groups, group)
	r.removeConnectionFromIndex(r.groupConnIndex, group, connKey)
	return nil
}

func (r *connectionRegistry) removeFromAllGroups(conn *Connection) {
	if conn == nil {
		return
	}
	connKey := connectionKey(conn)
	unlockConn := r.connLocks.lock(connKey)
	defer unlockConn()

	entry, ok := r.connections.Get(connKey)
	if !ok {
		return
	}
	for group := range entry.groups {
		r.removeConnectionFromIndex(r.groupConnIndex, group, connKey)
		delete(entry.groups, group)
	}
}

func (r *connectionRegistry) touch(conn *Connection) {
	if conn == nil {
		return
	}
	connKey := connectionKey(conn)
	entry, ok := r.connections.Get(connKey)
	if !ok || entry.conn != conn {
		return
	}
	entry.lastSeen.Store(time.Now().UnixNano())
}

func (r *connectionRegistry) expired(now time.Time, timeout time.Duration) []*Connection {
	if timeout <= 0 {
		return nil
	}
	deadlineNano := now.UnixNano() - int64(timeout)

	var expiredKeys []string
	var expiredEntries []*registryEntry
	r.connections.Range(func(key string, entry *registryEntry) bool {
		if entry != nil && entry.conn != nil && entry.lastSeen.Load() < deadlineNano {
			expiredKeys = append(expiredKeys, key)
			expiredEntries = append(expiredEntries, entry)
		}
		return true
	})

	if len(expiredKeys) == 0 {
		return nil
	}

	expiredConns := make([]*Connection, 0, len(expiredKeys))
	for i, key := range expiredKeys {
		entry := expiredEntries[i]
		unlock := r.connLocks.lock(key)
		current, ok := r.connections.Get(key)
		if !ok || current != entry || entry.lastSeen.Load() >= deadlineNano {
			unlock()
			continue
		}
		r.connections.Delete(key)
		if entry.conn.UserID != "" {
			r.removeConnectionFromIndex(r.userConnIndex, entry.conn.UserID, key)
		}
		for group := range entry.groups {
			r.removeConnectionFromIndex(r.groupConnIndex, group, key)
		}
		unlock()
		expiredConns = append(expiredConns, entry.conn)
	}
	return expiredConns
}

func (r *connectionRegistry) nextExpiration(timeout time.Duration) (time.Duration, bool) {
	if timeout <= 0 {
		return 0, false
	}
	if r.connections.Len() == 0 {
		return 0, false
	}
	return timeout / 2, true
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

func newConnectionMap() *mapx.ShardMap[string, *registryEntry] {
	return mapx.NewShardMap(mapx.ShardMapOptions[string, *registryEntry]{
		ShardNum: registryShardNum,
	})
}

func newConnectionSetIndex() *mapx.ShardMap[string, *connectionSet] {
	return mapx.NewShardMap(mapx.ShardMapOptions[string, *connectionSet]{
		ShardNum: registryShardNum,
	})
}

func newConnectionSet() *connectionSet {
	return mapx.NewShardMap(mapx.ShardMapOptions[string, *Connection]{
		ShardNum: registryShardNum,
	})
}

func connectionKey(conn *Connection) string {
	if conn.ID == "" {
		return fmt.Sprintf("%p", conn)
	}
	return conn.ID
}

func normalizeGroup(group string) string {
	return strings.TrimSpace(group)
}

type stripedMutexes struct {
	locks [registryShardNum]sync.Mutex
}

func (s *stripedMutexes) lock(key string) func() {
	mutex := &s.locks[lockShard(key)]
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
