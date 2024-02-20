```mermaid

erDiagram

    WORKSPACE {
        varchar name
        uint id PK
        varchar external_id
        datetime created_at
        datetime updated_at
    }

    TOKEN {
        uint id PK
        varchar external_id
        varchar key
        datetime created_at
        datetime updated_at
        bool active
        bool resuable
        uint workspace_id FK
    }

    VOLUME {
        uint id PK
        varchar external_id
        varchar name
        datetime created_at
        uint workspace_id FK
    }

    DEPLOYMENT {
        uint id PK
        varchar external_id
        uint version
        enum status
        datetime created_at
        datetime updated_at
        uint stub_id FK
        uint workspace_id FK
    }

    TASK {
        uint id PK
        varchar external_id
        datetime created_at
        datetime started_at
        bool ended_at
        uint stub_id FK
        uint workspace_id FK
    }

    STUB {
        uint id PK
        varchar external_id
        varchar name
        enum type
        JSON config
        uint object_id FK
        uint workspace_id FK
        datetime created_at
        datetime updated_at
    }

    OBJECT {
        uint id PK
        varchar external_id
        varchar hash
        int64 size
        datetime created_at
        uint workspace_id FK
    }

    WORKSPACE ||--o{ TOKEN : " "
    WORKSPACE ||--o{ VOLUME : " "
    WORKSPACE ||--o{ DEPLOYMENT : " "
    WORKSPACE ||--o{ TASK : " "
    WORKSPACE ||--o{ OBJECT : " "
    OBJECT ||--|| STUB : " "
    TASK ||--|| STUB : " "
    DEPLOYMENT ||--|| STUB : " "


```