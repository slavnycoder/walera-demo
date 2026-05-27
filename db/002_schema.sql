-- Showcase schema: todo_lists <- tasks <- subtasks.
-- Root entity for Walera subscriptions is todo_lists; every child mutation
-- bumps its parent in the same transaction (cascading up to todo_lists) so
-- subscribers of `todo_lists:<id>` receive every change in the subtree.

CREATE TABLE todo_lists (
    id          int8        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title       text        NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tasks (
    id            int8        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    todo_list_id  int8        NOT NULL REFERENCES todo_lists(id) ON DELETE CASCADE,
    title         text        NOT NULL,
    status        text        NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED')),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE subtasks (
    id          int8        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id     int8        NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    title       text        NOT NULL,
    status      text        NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED')),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Root-bump: tasks change -> bump todo_lists.updated_at (the routing anchor).
CREATE OR REPLACE FUNCTION bump_todo_lists_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE todo_lists
       SET updated_at = now()
     WHERE id = COALESCE(NEW.todo_list_id, OLD.todo_list_id);
    RETURN COALESCE(NEW, OLD);
END $$;

CREATE TRIGGER tasks_bump_todo_lists
    AFTER INSERT OR UPDATE OR DELETE ON tasks
    FOR EACH ROW EXECUTE FUNCTION bump_todo_lists_updated_at();

-- Depth-3: subtasks change -> bump parent tasks.updated_at, which cascades
-- via the trigger above up to todo_lists.updated_at in the same transaction.
CREATE OR REPLACE FUNCTION bump_tasks_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE tasks
       SET updated_at = now()
     WHERE id = COALESCE(NEW.task_id, OLD.task_id);
    RETURN COALESCE(NEW, OLD);
END $$;

CREATE TRIGGER subtasks_bump_tasks
    AFTER INSERT OR UPDATE OR DELETE ON subtasks
    FOR EACH ROW EXECUTE FUNCTION bump_tasks_updated_at();

ALTER PUBLICATION cdc_sse_streamer
    ADD TABLE todo_lists, tasks, subtasks;

-- Deterministic seed so the frontend can subscribe to todo_lists:1.
INSERT INTO todo_lists (id, title) OVERRIDING SYSTEM VALUE VALUES
    (1, 'Walera showcase TODO');

INSERT INTO tasks (id, todo_list_id, title) OVERRIDING SYSTEM VALUE VALUES
    (1, 1, 'Design schema'),
    (2, 1, 'Implement backend'),
    (3, 1, 'Build frontend');

INSERT INTO subtasks (id, task_id, title) OVERRIDING SYSTEM VALUE VALUES
    (1, 1, 'Draft tables'),
    (2, 1, 'Add cascading triggers'),
    (3, 2, 'Wire API routes'),
    (4, 2, 'Expose auth endpoint'),
    (5, 3, 'Render HTML'),
    (6, 3, 'Hook up SSE');

SELECT setval(pg_get_serial_sequence('public.todo_lists', 'id'), (SELECT MAX(id) FROM todo_lists));
SELECT setval(pg_get_serial_sequence('public.tasks',      'id'), (SELECT MAX(id) FROM tasks));
SELECT setval(pg_get_serial_sequence('public.subtasks',   'id'), (SELECT MAX(id) FROM subtasks));
