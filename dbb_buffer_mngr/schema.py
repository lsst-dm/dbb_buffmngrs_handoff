SCHEMA = {
    "type": "object",
    "properties": {
        "local": {
            "type": "object",
            "properties": {
                "buffer": {"type": "string"},
                "storage": {"type": "string"}
            },
            "required": ["buffer", "storage"]
        },
        "remote": {
            "type": "object",
            "properties": {
                "user": {"type": "string"},
                "host": {"type": "string"},
                "path": {"type": "string"}
            },
            "required": ["user", "host", "path"]
        },
        "logging": {
            "type": "object",
            "properties": {
                "logfile": {"type": "string"},
                "loglevel": {
                    "type": "string",
                    "enum": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
                },
            }
        },
        "general": {
            "type": "object",
            "properties": {
                "chunk_size": {
                    "type": "integer",
                    "minimum": 1
                },
                "porters": {
                    "type": "integer",
                    "minimum": 1
                },
                "delay": {
                    "type": "integer",
                    "minimum": 1
                }
            }
        }
    },
    "required": ["local", "remote"]
}
