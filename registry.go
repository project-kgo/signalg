package signalg

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kanengo/ku/mapx"
	"github.com/kanengo/ku/poolx/slicepool"
)

const registryShardNum = 64

type connectionSet = mapx.ShardMap[string, *Connection]
type groupSet = mapx.ShardMap[string, struct{}]

var connectionSlicePool = &slicepool.Pool[*Connection]{}

type connectionRegistry struct {
	connections    *mapx.ShardMap[string, *Connection]
	userConnIndex  *mapx.ShardMap[string, *connectionSet]
	groupConnIndex *mapx.ShardMap[string, *connectionSet]
	connGroupIndex *mapx.ShardMap[string, *groupSet]

	connLocks      stripedMutexes
	indexInitLocks stripedMutexes
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

	r.connections.Set(connKey, conn)
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

	registeredConn, ok := r.connections.Get(connKey)
	if !ok || registeredConn != conn {
		return false
	}
	r.connections.Delete(connKey)
	if registeredConn.UserID != "" {
		r.removeConnectionFromIndex(r.userConnIndex, registeredConn.UserID, connKey)
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
	return r.connections.Values()
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

	registeredConn, ok := r.connections.Get(connKey)
	if !ok || registeredConn != conn {
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

	registeredConn, ok := r.connections.Get(connKey)
	if !ok || registeredConn != conn {
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

func newConnectionMap() *mapx.ShardMap[string, *Connection] {
	return mapx.NewShardMap[string, *Connection](mapx.ShardMapOptions[string, *Connection]{
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

type stripedMutexes struct {
	locks [registryShardNum]sync.Mutex
}

func (s *stripedMutexes) lock(key string) func() {
	mutex := &s.locks[lockShard(key)]
	mutex.Lock()
	return mutex.Unlock
}

func lockShard(key string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime64
	}
	return hash % registryShardNum
}

func normalizeGroup(group string) string {
	return strings.TrimSpace(group)
}
