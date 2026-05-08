package signalg

import (
	"strings"
	"sync"
)

type connectionRegistry struct {
	mu             sync.RWMutex
	connections    map[*Connection]struct{}
	userConnIndex  map[string]map[*Connection]struct{}
	groupConnIndex map[string]map[*Connection]struct{}
	connGroupIndex map[*Connection]map[string]struct{}
}

func newConnectionRegistry() *connectionRegistry {
	return &connectionRegistry{
		connections:    make(map[*Connection]struct{}),
		userConnIndex:  make(map[string]map[*Connection]struct{}),
		groupConnIndex: make(map[string]map[*Connection]struct{}),
		connGroupIndex: make(map[*Connection]map[string]struct{}),
	}
}

func (r *connectionRegistry) add(conn *Connection) {
	if conn == nil {
		return
	}
	r.mu.Lock()
	r.addLocked(conn)
	r.mu.Unlock()
}

func (r *connectionRegistry) addLocked(conn *Connection) {
	r.connections[conn] = struct{}{}
	if conn.UserID != "" {
		userConnections := r.userConnIndex[conn.UserID]
		if userConnections == nil {
			userConnections = make(map[*Connection]struct{})
			r.userConnIndex[conn.UserID] = userConnections
		}
		userConnections[conn] = struct{}{}
	}
}

func (r *connectionRegistry) remove(conn *Connection) bool {
	if conn == nil {
		return false
	}
	r.mu.Lock()
	_, ok := r.connections[conn]
	if !ok {
		r.mu.Unlock()
		return false
	}
	delete(r.connections, conn)
	if conn.UserID != "" {
		userConnections := r.userConnIndex[conn.UserID]
		delete(userConnections, conn)
		if len(userConnections) == 0 {
			delete(r.userConnIndex, conn.UserID)
		}
	}
	for group := range r.connGroupIndex[conn] {
		groupConnections := r.groupConnIndex[group]
		delete(groupConnections, conn)
		if len(groupConnections) == 0 {
			delete(r.groupConnIndex, group)
		}
	}
	delete(r.connGroupIndex, conn)
	r.mu.Unlock()
	return true
}

func (r *connectionRegistry) allConnections() []*Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.allConnectionsLocked()
}

func (r *connectionRegistry) allConnectionsLocked() []*Connection {
	connections := make([]*Connection, 0, len(r.connections))
	for conn := range r.connections {
		connections = append(connections, conn)
	}
	return connections
}

func (r *connectionRegistry) userOnline(userID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.userConnIndex[userID])
}

func (r *connectionRegistry) userConnections(userID string) []*Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return copyConnectionSet(r.userConnIndex[userID])
}

func (r *connectionRegistry) userSnapshot(userIDs []string) []*Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(userIDs) == 0 {
		return nil
	}
	if len(userIDs) == 1 {
		userID := userIDs[0]
		if userID == "" {
			return nil
		}
		return copyConnectionSet(r.userConnIndex[userID])
	}

	total := 0
	for _, userID := range userIDs {
		if userID == "" {
			continue
		}
		userConnections := r.userConnIndex[userID]
		total += len(userConnections)
	}
	if total == 0 {
		return nil
	}

	connections := make([]*Connection, 0, total)
	seen := make(map[*Connection]struct{}, total)
	for _, userID := range userIDs {
		if userID == "" {
			continue
		}
		userConnections := r.userConnIndex[userID]
		for conn := range userConnections {
			if _, ok := seen[conn]; ok {
				continue
			}
			seen[conn] = struct{}{}
			connections = append(connections, conn)
		}
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

	r.mu.Lock()
	if _, ok := r.connections[conn]; !ok {
		r.mu.Unlock()
		return ErrConnectionNotFound
	}
	groupConnections := r.groupConnIndex[group]
	if groupConnections == nil {
		groupConnections = make(map[*Connection]struct{})
		r.groupConnIndex[group] = groupConnections
	}
	groupConnections[conn] = struct{}{}

	connGroups := r.connGroupIndex[conn]
	if connGroups == nil {
		connGroups = make(map[string]struct{})
		r.connGroupIndex[conn] = connGroups
	}
	connGroups[group] = struct{}{}
	r.mu.Unlock()
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

	r.mu.Lock()
	if _, ok := r.connections[conn]; !ok {
		r.mu.Unlock()
		return ErrConnectionNotFound
	}
	groupConnections := r.groupConnIndex[group]
	delete(groupConnections, conn)
	if len(groupConnections) == 0 {
		delete(r.groupConnIndex, group)
	}
	connGroups := r.connGroupIndex[conn]
	delete(connGroups, group)
	if len(connGroups) == 0 {
		delete(r.connGroupIndex, conn)
	}
	r.mu.Unlock()
	return nil
}

func (r *connectionRegistry) removeFromAllGroups(conn *Connection) {
	if conn == nil {
		return
	}
	r.mu.Lock()
	for group := range r.connGroupIndex[conn] {
		groupConnections := r.groupConnIndex[group]
		delete(groupConnections, conn)
		if len(groupConnections) == 0 {
			delete(r.groupConnIndex, group)
		}
	}
	delete(r.connGroupIndex, conn)
	r.mu.Unlock()
}

func (r *connectionRegistry) groupOnline(group string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.groupConnIndex[group])
}

func (r *connectionRegistry) groupConnections(group string) []*Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return copyConnectionSet(r.groupConnIndex[group])
}

func copyConnectionSet(set map[*Connection]struct{}) []*Connection {
	connections := make([]*Connection, 0, len(set))
	for conn := range set {
		connections = append(connections, conn)
	}
	return connections
}

func normalizeGroup(group string) string {
	return strings.TrimSpace(group)
}
