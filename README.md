# cordon_network
Analytics framework for checkpoint movement data
Two backends are available: networkx and neo4j

## General process
1. Identify cordon nodes and links and represent the structure with graph.
2. Identify moving objects and add them as nodes.
3. Determine whether every entry of the movement data maps to transaction or presence, add them as links in the graph structure.
4. Query with three types of operations (Atomic operation, proximity operation and subgraph operation) or raw Cypher quries.


