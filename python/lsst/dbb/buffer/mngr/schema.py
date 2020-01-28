# This file is part of dbb_buffer_mngr.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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