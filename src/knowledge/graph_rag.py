"""
GraphRAG 知识图谱检索增强生成系统

基于图结构的知识表示与推理
"""

from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import re
from collections import defaultdict

from src.utils.structured_logging import get_logger
from src.utils.thread_safe import ThreadSafeDict

logger = get_logger("graph_rag")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Entity:
    """知识图谱实体"""
    id: str
    name: str
    entity_type: str              # 设备、故障、部件、工艺参数等
    attributes: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    source: str = ""              # 数据来源
    confidence: float = 1.0       # 置信度
    created_at: datetime = field(default_factory=utc_now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "entity_type": self.entity_type,
            "attributes": self.attributes,
            "description": self.description,
            "source": self.source,
            "confidence": self.confidence,
            "created_at": self.created_at.isoformat()
        }


@dataclass
class Relation:
    """知识图谱关系"""
    source_id: str
    target_id: str
    relation_type: str            # 导致、关联、解决、属于等
    attributes: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 1.0
    bidirectional: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "relation_type": self.relation_type,
            "attributes": self.attributes,
            "confidence": self.confidence,
            "bidirectional": self.bidirectional
        }


@dataclass
class KnowledgePath:
    """知识路径（用于推理）"""
    entities: List[Entity]
    relations: List[Relation]
    path_score: float = 0.0
    reasoning_chain: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "entities": [e.to_dict() for e in self.entities],
            "relations": [r.to_dict() for r in self.relations],
            "path_score": self.path_score,
            "reasoning_chain": self.reasoning_chain
        }


class KnowledgeGraph:
    """知识图谱"""
    
    # 工业领域本体定义
    ENTITY_TYPES = {
        "device": "设备",
        "component": "部件",
        "fault": "故障",
        "symptom": "症状",
        "cause": "原因",
        "solution": "解决方案",
        "parameter": "工艺参数",
        "material": "物料",
        "process": "工艺过程"
    }
    
    RELATION_TYPES = {
        "causes": "导致",
        "belongs_to": "属于",
        "has_part": "包含部件",
        "manifests_as": "表现为",
        "solved_by": "可通过...解决",
        "associated_with": "关联",
        "affects": "影响",
        "requires": "需要",
        "similar_to": "类似于"
    }
    
    def __init__(self, name: str = "industrial_kg"):
        self.name = name
        self._entities: Dict[str, Entity] = {}
        self._relations: List[Relation] = []
        self._entity_index: Dict[str, Set[str]] = defaultdict(set)  # type -> ids
        self._adjacency: Dict[str, List[Tuple[str, str]]] = defaultdict(list)  # node -> [(target, relation)]
        
        # 初始化示例知识
        self._init_industrial_knowledge()
    
    def _init_industrial_knowledge(self):
        """初始化工业领域知识"""
        # 设备实体
        devices = [
            Entity("DEV_001", "曝气机", "device", 
                  {"type": "离心风机", "power": "15kW"},
                  "污水处理曝气设备"),
            Entity("DEV_002", "污泥泵", "device",
                  {"type": "离心泵", "flow": "100m³/h"},
                  "污泥回流泵"),
            Entity("DEV_003", "DO传感器", "device",
                  {"type": "光学式", "range": "0-20mg/L"},
                  "溶解氧在线监测仪")
        ]
        
        # 故障实体
        faults = [
            Entity("FAULT_001", "曝气不足", "fault",
                  {"severity": "high", "frequency": "common"},
                  "曝气量不能满足工艺需求"),
            Entity("FAULT_002", "轴承过热", "fault",
                  {"severity": "medium", "frequency": "occasional"},
                  "轴承温度超过正常范围"),
            Entity("FAULT_003", "读数漂移", "fault",
                  {"severity": "low", "frequency": "common"},
                  "传感器测量值不稳定")
        ]
        
        # 原因实体
        causes = [
            Entity("CAUSE_001", "曝气盘堵塞", "cause",
                  {"mechanism": "污泥沉积"},
                  "曝气孔被污泥堵塞"),
            Entity("CAUSE_002", "润滑不良", "cause",
                  {"mechanism": "油脂老化"},
                  "轴承润滑油脂失效"),
            Entity("CAUSE_003", "探头污染", "cause",
                  {"mechanism": "生物附着"},
                  "传感器探头表面被污染物覆盖")
        ]
        
        # 解决方案实体
        solutions = [
            Entity("SOL_001", "清洗曝气盘", "solution",
                  {"difficulty": "medium", "cost": "low"},
                  "拆下曝气盘进行清洗"),
            Entity("SOL_002", "更换润滑脂", "solution",
                  {"difficulty": "low", "cost": "low"},
                  "排出旧油脂，添加新润滑脂"),
            Entity("SOL_003", "校准传感器", "solution",
                  {"difficulty": "low", "cost": "minimal"},
                  "清洗探头并进行零点/量程校准")
        ]
        
        # 添加实体
        for entity in devices + faults + causes + solutions:
            self.add_entity(entity)
        
        # 添加关系
        relations = [
            # 故障-原因关系
            Relation("FAULT_001", "CAUSE_001", "causes"),
            Relation("FAULT_002", "CAUSE_002", "causes"),
            Relation("FAULT_003", "CAUSE_003", "causes"),
            
            # 原因-解决方案关系
            Relation("CAUSE_001", "SOL_001", "solved_by"),
            Relation("CAUSE_002", "SOL_002", "solved_by"),
            Relation("CAUSE_003", "SOL_003", "solved_by"),
            
            # 设备-故障关系
            Relation("DEV_001", "FAULT_001", "manifests_as"),
            Relation("DEV_002", "FAULT_002", "manifests_as"),
            Relation("DEV_003", "FAULT_003", "manifests_as")
        ]
        
        for relation in relations:
            self.add_relation(relation)
        
        logger.info(f"知识图谱初始化完成: {len(self._entities)} 实体, {len(self._relations)} 关系")
    
    def add_entity(self, entity: Entity) -> str:
        """添加实体"""
        self._entities[entity.id] = entity
        self._entity_index[entity.entity_type].add(entity.id)
        return entity.id
    
    def add_relation(self, relation: Relation):
        """添加关系"""
        self._relations.append(relation)
        self._adjacency[relation.source_id].append((relation.target_id, relation.relation_type))
        if relation.bidirectional:
            self._adjacency[relation.target_id].append((relation.source_id, relation.relation_type))
    
    def get_entity(self, entity_id: str) -> Optional[Entity]:
        """获取实体"""
        return self._entities.get(entity_id)
    
    def get_entities_by_type(self, entity_type: str) -> List[Entity]:
        """按类型获取实体"""
        return [self._entities[eid] for eid in self._entity_index.get(entity_type, [])]
    
    def search_entities(self, keyword: str, entity_type: Optional[str] = None) -> List[Entity]:
        """搜索实体"""
        results = []
        keyword_lower = keyword.lower()
        
        for entity in self._entities.values():
            if entity_type and entity.entity_type != entity_type:
                continue
            
            # 匹配名称和描述
            if (keyword_lower in entity.name.lower() or 
                keyword_lower in entity.description.lower()):
                results.append(entity)
        
        return results
    
    def get_neighbors(self, entity_id: str, relation_type: Optional[str] = None) -> List[Tuple[Entity, str]]:
        """获取邻居实体"""
        neighbors = []
        for target_id, rel_type in self._adjacency.get(entity_id, []):
            if relation_type and rel_type != relation_type:
                continue
            target = self._entities.get(target_id)
            if target:
                neighbors.append((target, rel_type))
        return neighbors
    
    def find_paths(self, start_id: str, end_id: str, max_depth: int = 3) -> List[KnowledgePath]:
        """
        查找实体间的路径
        
        用于多跳推理
        """
        paths = []
        visited = set()
        
        def dfs(current_id: str, target_id: str, path_entities: List[Entity], 
                path_relations: List[Relation], depth: int):
            if depth > max_depth:
                return
            
            if current_id == target_id and len(path_entities) > 1:
                paths.append(KnowledgePath(
                    entities=path_entities.copy(),
                    relations=path_relations.copy(),
                    path_score=self._calculate_path_score(path_relations)
                ))
                return
            
            visited.add(current_id)
            
            for next_id, rel_type in self._adjacency.get(current_id, []):
                if next_id not in visited:
                    next_entity = self._entities.get(next_id)
                    if next_entity:
                        relation = Relation(current_id, next_id, rel_type)
                        path_entities.append(next_entity)
                        path_relations.append(relation)
                        
                        dfs(next_id, target_id, path_entities, path_relations, depth + 1)
                        
                        path_entities.pop()
                        path_relations.pop()
            
            visited.remove(current_id)
        
        start_entity = self._entities.get(start_id)
        if start_entity:
            dfs(start_id, end_id, [start_entity], [], 0)
        
        # 按路径得分排序
        paths.sort(key=lambda p: p.path_score, reverse=True)
        return paths
    
    def _calculate_path_score(self, relations: List[Relation]) -> float:
        """计算路径得分"""
        if not relations:
            return 0.0
        
        # 基于关系置信度和路径长度计算
        confidence_product = 1.0
        for rel in relations:
            confidence_product *= rel.confidence
        
        # 路径越短得分越高
        length_penalty = 1.0 / (1 + len(relations) * 0.1)
        
        return confidence_product * length_penalty
    
    def subgraph_query(self, center_entity_id: str, depth: int = 2) -> Dict[str, Any]:
        """
        子图查询
        
        获取实体周围的局部知识图谱
        """
        nodes = set()
        edges = []
        
        # BFS遍历
        queue = [(center_entity_id, 0)]
        visited = {center_entity_id}
        
        while queue:
            current_id, current_depth = queue.pop(0)
            nodes.add(current_id)
            
            if current_depth < depth:
                for next_id, rel_type in self._adjacency.get(current_id, []):
                    edges.append({
                        "source": current_id,
                        "target": next_id,
                        "relation": rel_type
                    })
                    
                    if next_id not in visited:
                        visited.add(next_id)
                        queue.append((next_id, current_depth + 1))
        
        return {
            "nodes": [self._entities[nid].to_dict() for nid in nodes if nid in self._entities],
            "edges": edges
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """导出图谱"""
        return {
            "name": self.name,
            "entities": [e.to_dict() for e in self._entities.values()],
            "relations": [r.to_dict() for r in self._relations],
            "stats": {
                "entity_count": len(self._entities),
                "relation_count": len(self._relations),
                "entity_types": {t: len(ids) for t, ids in self._entity_index.items()}
            }
        }


class GraphRAG:
    """
    GraphRAG 检索增强生成系统
    
    结合知识图谱的RAG实现
    """
    
    def __init__(self, knowledge_graph: Optional[KnowledgeGraph] = None):
        self.kg = knowledge_graph or KnowledgeGraph()
        self.llm_client = None  # 可接入真实LLM
    
    async def retrieve(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        检索相关知识
        
        1. 实体匹配
        2. 子图扩展
        3. 路径推理
        """
        results = []
        
        # 1. 实体识别（简化版，实际应使用NER）
        entities = self._extract_entities(query)
        
        # 2. 对每个实体进行子图查询
        for entity_name in entities:
            matched = self.kg.search_entities(entity_name)
            for entity in matched[:3]:  # 每个实体最多3个匹配
                subgraph = self.kg.subgraph_query(entity.id, depth=2)
                results.append({
                    "type": "subgraph",
                    "entity": entity.to_dict(),
                    "subgraph": subgraph,
                    "relevance_score": self._calculate_relevance(query, entity)
                })
        
        # 3. 如果实体匹配不足，进行全文搜索
        if len(results) < top_k:
            all_entities = list(self.kg._entities.values())
            for entity in all_entities:
                score = self._calculate_relevance(query, entity)
                if score > 0.3:  # 阈值
                    results.append({
                        "type": "entity",
                        "entity": entity.to_dict(),
                        "relevance_score": score
                    })
        
        # 按相关度排序
        results.sort(key=lambda x: x["relevance_score"], reverse=True)
        return results[:top_k]
    
    def _extract_entities(self, text: str) -> List[str]:
        """从文本中提取实体（简化版）"""
        # 使用规则匹配常见工业实体
        entity_keywords = [
            "曝气机", "风机", "泵", "传感器", "轴承", "电机",
            "DO", "溶解氧", "污泥", "温度", "压力", "流量",
            "堵塞", "过热", "漂移", "磨损", "泄漏"
        ]
        
        found = []
        for keyword in entity_keywords:
            if keyword in text:
                found.append(keyword)
        
        return found
    
    def _calculate_relevance(self, query: str, entity: Entity) -> float:
        """计算查询与实体的相关度"""
        query_lower = query.lower()
        name_lower = entity.name.lower()
        desc_lower = entity.description.lower()
        
        score = 0.0
        
        # 名称匹配权重最高
        if name_lower in query_lower or query_lower in name_lower:
            score += 0.5
        
        # 描述匹配
        common_words = set(query_lower.split()) & set(desc_lower.split())
        score += len(common_words) * 0.1
        
        # 属性匹配
        for attr_value in entity.attributes.values():
            if str(attr_value).lower() in query_lower:
                score += 0.1
        
        return min(score, 1.0)
    
    async def generate(self, query: str, context: List[Dict[str, Any]]) -> str:
        """
        基于检索结果生成回答
        
        实际应调用LLM，这里使用模板生成
        """
        # 构建提示
        prompt = self._build_prompt(query, context)
        
        # 模拟生成（实际应调用LLM）
        response = self._mock_generate(prompt, context)
        
        return response
    
    def _build_prompt(self, query: str, context: List[Dict[str, Any]]) -> str:
        """构建生成提示"""
        prompt = f"""基于以下知识图谱信息回答问题。

问题：{query}

相关知识：
"""
        
        for i, item in enumerate(context, 1):
            if item["type"] == "subgraph":
                entity = item["entity"]
                prompt += f"\n{i}. {entity['name']} ({entity['entity_type']})\n"
                prompt += f"   描述：{entity['description']}\n"
                prompt += f"   属性：{json.dumps(entity['attributes'], ensure_ascii=False)}\n"
            else:
                entity = item["entity"]
                prompt += f"\n{i}. {entity['name']}\n"
                prompt += f"   {entity['description']}\n"
        
        prompt += "\n请基于以上知识给出详细回答："
        return prompt
    
    def _mock_generate(self, query: str, context: List[Dict[str, Any]]) -> str:
        """模拟生成回答"""
        if not context:
            return "抱歉，知识库中没有找到相关信息。"
        
        # 提取关键信息构建回答
        entities_info = []
        for item in context[:3]:
            entity = item["entity"]
            entities_info.append(f"{entity['name']}({entity['entity_type']})")
        
        response = f"根据知识图谱分析，涉及以下实体：{', '.join(entities_info)}。"
        response += "\n\n详细分析：\n"
        
        for item in context:
            entity = item["entity"]
            response += f"\n• {entity['name']}: {entity['description']}"
            
            # 添加相关实体信息
            if item["type"] == "subgraph":
                neighbors = item["subgraph"].get("edges", [])
                if neighbors:
                    response += "\n  相关关系："
                    for edge in neighbors[:3]:
                        response += f"{edge['relation']} -> {edge['target']}; "
        
        return response
    
    async def query(self, query: str) -> Dict[str, Any]:
        """
        完整的GraphRAG查询流程
        """
        # 1. 检索
        retrieved = await self.retrieve(query)
        
        # 2. 生成
        answer = await self.generate(query, retrieved)
        
        return {
            "query": query,
            "retrieved_context": retrieved,
            "answer": answer,
            "sources": [item["entity"]["id"] for item in retrieved if "entity" in item]
        }
    
    def add_knowledge_from_text(self, text: str, source: str = "manual"):
        """
        从文本中添加知识到图谱
        
        简化版实现，实际应使用信息抽取模型
        """
        # 这里可以实现实体关系抽取逻辑
        logger.info(f"从文本添加知识: {text[:50]}...")
        
        # 示例：识别设备-故障关系
        device_patterns = [
            (r"(\w+).*?曝气不足", "FAULT_001"),
            (r"(\w+).*?轴承过热", "FAULT_002"),
            (r"(\w+).*?读数漂移", "FAULT_003")
        ]
        
        for pattern, fault_id in device_patterns:
            match = re.search(pattern, text)
            if match:
                device_name = match.group(1)
                # 创建设备实体
                device_id = f"DEV_{device_name}_{len(self.kg._entities)}"
                device = Entity(device_id, device_name, "device", {}, source=source)
                self.kg.add_entity(device)
                
                # 添加故障关系
                relation = Relation(device_id, fault_id, "manifests_as")
                self.kg.add_relation(relation)
                
                logger.info(f"添加知识: {device_name} -> {fault_id}")


# 全局GraphRAG实例
graph_rag = GraphRAG()


async def test_graph_rag():
    """测试GraphRAG"""
    # 查询示例
    result = await graph_rag.query("曝气机轴承过热怎么办？")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_graph_rag())
