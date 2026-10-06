-- name: ListEquipment :many
SELECT id, name, type, created_at
FROM equipment
ORDER BY id;
