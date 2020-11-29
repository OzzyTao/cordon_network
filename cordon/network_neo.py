from neo4j import GraphDatabase

def construct_query_labels(properties, prefix=""):
    result = ""
    for key in properties:
        result += key + ':$'+key+' '
    result = result + prefix
    return '{'+result+'}' if result else result

class GraphDBModel:
    ''' nodes: CNode, CLink, Mover ;
        relationships: PASSES, STAYS, LINKS '''
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_cordon_network(self, regions, links, connectivity):
        '''regions are a dict of dicts which give infos about Cordon nodes, each node is referenced with its id;
            links are a dict of dicts which give infos about cordon edges, each link is referenced with its id;
            connectivity is a list of tuples (region1, link, region2) indicates
            the connectivity between two regions through on link'''
        with self.driver.session() as session:
            for region_id in regions.keys():
                result = session.write_transaction(GraphDBModel._find_or_create_cordon_node,region_id, **regions[region_id])
        with self.driver.session() as session:
            for link_id in links.keys():
                result = session.write_transaction(GraphDBModel._find_or_create_cordon_link,link_id,**links[link_id])
        with self.driver.session() as session:
            for t in connectivity:
                result = session.write_transaction(GraphDBModel._find_or_create_cordon_relationship, t[0], t[1], t[2])
    @staticmethod
    def _find_or_create_cordon_node(tx, id, **kwargs):
        query = (
            "MERGE (n:CNode "+construct_query_labels(['id']+kwargs.keys())+")"
            "RETURN n"
        )
        result = tx.run(query, id=id, **kwargs)
        return result

    @staticmethod
    def _find_or_create_cordon_link(tx, id, **kwargs):
        query = (
                "MERGE (n:CLink " + construct_query_labels(['id']+kwargs.keys()) + ")"
                "RETURN n"
        )
        result = tx.run(query, id=id, **kwargs)
        return result

    @staticmethod
    def _find_or_create_cordon_relationship(tx, region1, link, region2):
        query = (
            "MERGE (r1:CNode {id:$region1})-[s1:LINKS]-(l1:CLink {id:$link})-[s2:LINKS]-(r2:CNode {id:$region2})"
            "RETURN s1"
        )
        result = tx.run(query, region1=region1,link=link, region2=region2)
        return result

    def add_movers(self,movers):
        '''movers is a dict of dicts with mover id being keys'''
        with self.driver.session() as session:
            for mover_id in movers.keys():
                result = session.write_transaction(GraphDBModel._find_or_create_mover,mover_id,**movers[mover_id])

    @staticmethod
    def _find_or_create_mover(tx, id, **kwargs):
        query = (
            "MERGE (m:Mover "+construct_query_labels(['id']+kwargs.keys())+")"
            "RETURN m"
        )
        result = tx.run(query, id=id, **kwargs)
        return result

    def add_presence_records(self, records):
        '''records are a list of dicts that each dict contains at least mover_id, CNode_id, enter_time, exit_time'''
        with self.driver.session() as session:
            for record in records:
                r = record.copy()
                mover_id = r.pop('mover_id', None)
                cnode_id = r.pop('CNode_id', None)
                enterTime = r.pop('enter_time', None)
                exitTime = r.pop('exit_time',None)
                result = session.write_transaction(GraphDBModel._find_or_create_presence, mover_id, cnode_id, enterTime, exitTime, **r)

    @staticmethod
    def _find_or_create_presence(tx, mover_id, cnode_id, enterTime, exitTime, **kwargs):
        query = (
            "MERGE (m:Mover {id:$mover_id})-[s:STAYS "
            +construct_query_labels(kwargs, prefix="enterTime:datetime($enterTime) exitTime:datetime($exitTime)")+
            "]->(c:CNode {id:$cnode_id})"
            "RETURN s"
        )
        result = tx.run(query, mover_id=mover_id,cnode_id=cnode_id,enterTime=enterTime,exitTime=exitTime, **kwargs)
        return result

    def add_transaction_records(self, records):
        ''' records are a list of dicts that each contains at least mover_id, CLink_id, t (time)'''
        with self.driver.session() as session:
            for record in records:
                r = record.copy()
                mover_id = r.pop('mover_id',None)
                clink_id = r.pop('CLink_id',None)
                t = r.pop('t',None)
                result = session.write_transaction(GraphDBModel._find_or_create_cordon_link,mover_id,clink_id,t,**r)

    @staticmethod
    def _find_or_create_transaction(tx, mover_id, clink_id, t, **kwargs):
        query = (
            "MERGE (m:Mover {id:$mover_id})-[p:PASSES "+
            construct_query_labels(kwargs, prefix="t:datetime($t)")+
            "]->(l:CLink {id:$clink_id})"
            "RETURN p"
        )
        result = tx.run(query, mover_id=mover_id,clink_id=clink_id,t=t,**kwargs)
        return result


    def query(self, q, **kwargs):
        with self.driver.session() as session:
            return session.read_transaction(GraphDBModel._query, q, **kwargs)

    @staticmethod
    def _query(tx, q, **kwargs):
        result = tx.run(q, **kwargs)
        return result