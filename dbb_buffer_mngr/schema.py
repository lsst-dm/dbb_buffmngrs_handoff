SCHEMA = {
    "type": "object",
    "properties": {
        "handoff": {
            "type": "object",
            "properties": {
                "buffer": {"type": "string"},
                "holding": {"type": "string"}
            },
            "required": ["buffer", "holding"]
        },
        "endpoint": {
            "type": "object",
            "properties": {
                "user": {"type": "string"},
                "host": {"type": "string"},
                "buffer": {"type": "string"},
                "staging": {"type": "string"}
            },
            "required": ["user", "host", "buffer", "staging"]
        },
        "logging": {
            "type": "object",
            "properties": {
                "file": {"type": "string"},
                "level": {
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
    "required": ["handoff", "endpoint"]
}
