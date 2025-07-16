// MetaZenseCode Memgraph Initialization Script
// This script sets up the initial database schema and indexes

// Create indexes for better performance
CREATE INDEX ON :PIPELINE(id);
CREATE INDEX ON :OPERATION(id);
CREATE INDEX ON :DATA_ASSET(id);
CREATE INDEX ON :CONNECTION(id);
CREATE INDEX ON :PARAMETER(id);
CREATE INDEX ON :VARIABLE(id);
CREATE INDEX ON :TABLE(id);
CREATE INDEX ON :COLUMN(id);

// Create constraints for data integrity
CREATE CONSTRAINT ON (p:PIPELINE) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (o:OPERATION) ASSERT o.id IS UNIQUE;
CREATE CONSTRAINT ON (d:DATA_ASSET) ASSERT d.id IS UNIQUE;
CREATE CONSTRAINT ON (c:CONNECTION) ASSERT c.id IS UNIQUE;

// Log initialization completion
RETURN "MetaZenseCode Memgraph database initialized successfully" AS status;