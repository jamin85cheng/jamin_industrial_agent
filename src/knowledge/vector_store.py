"""
向量存储模块

功能：
- 文档向量化 (Embedding)
- 向量相似度搜索
- 向量数据库集成 (ChromaDB, FAISS)
- 持久化存储

支持的后端：
- ChromaDB (推荐，轻量级)
- FAISS (Facebook AI Similarity Search)
- 内存向量索引 (开发测试用)
"""

import numpy as np
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
import json
import os
from loguru import logger


class VectorStore:
    """向量存储抽象基类"""
    
    def add(self, texts: List[str], embeddings: Optional[List[List[float]]] = None,
            metadatas: Optional[List[Dict[str, Any]]] = None,
            ids: Optional[List[str]] = None) -> List[str]:
        """添加文档到向量库"""
        raise NotImplementedError
    
    def search(self, query: str, query_embedding: Optional[List[float]] = None,
               k: int = 5, filter_dict: Optional[Dict[str, Any]] = None) -> List[Tuple[str, float, Dict]]:
        """相似度搜索"""
        raise NotImplementedError
    
    def delete(self, ids: List[str]) -> bool:
        """删除文档"""
        raise NotImplementedError
    
    def count(self) -> int:
        """返回文档数量"""
        raise NotImplementedError


class MemoryVectorStore(VectorStore):
    """内存向量存储 (开发测试用)"""
    
    def __init__(self, embedding_function=None):
        self.embeddings = []
        self.texts = []
        self.metadatas = []
        self.ids = []
        self.embedding_function = embedding_function or self._default_embedding
    
    def _default_embedding(self, text: str) -> List[float]:
        """简单的词袋模型 embedding (仅用于测试)"""
        words = text.lower().split()
        embedding = np.zeros(100)
        for i, word in enumerate(words[:100]):
            embedding[i] = hash(word) % 1000 / 1000.0
        return embedding.tolist()
    
    def add(self, texts: List[str], embeddings: Optional[List[List[float]]] = None,
            metadatas: Optional[List[Dict[str, Any]]] = None,
            ids: Optional[List[str]] = None) -> List[str]:
        """添加文档"""
        added_ids = []
        
        for i, text in enumerate(texts):
            doc_id = ids[i] if ids else f"doc_{len(self.texts) + i}_{datetime.now().timestamp()}"
            emb = embeddings[i] if embeddings else self.embedding_function(text)
            metadata = metadatas[i] if metadatas else {}
            
            self.embeddings.append(emb)
            self.texts.append(text)
            self.metadatas.append(metadata)
            self.ids.append(doc_id)
            added_ids.append(doc_id)
        
        logger.info(f"内存向量库添加 {len(texts)} 个文档")
        return added_ids
    
    def search(self, query: str, query_embedding: Optional[List[float]] = None,
               k: int = 5, filter_dict: Optional[Dict[str, Any]] = None) -> List[Tuple[str, float, Dict]]:
        """余弦相似度搜索"""
        if not self.texts:
            return []
        
        # 计算查询向量
        if query_embedding is None:
            query_embedding = self.embedding_function(query)
        
        query_vec = np.array(query_embedding)
        
        # 计算所有文档的相似度
        similarities = []
        for i, doc_emb in enumerate(self.embeddings):
            doc_vec = np.array(doc_emb)
            
            # 余弦相似度
            similarity = np.dot(query_vec, doc_vec) / (np.linalg.norm(query_vec) * np.linalg.norm(doc_vec) + 1e-8)
            
            # 应用过滤
            if filter_dict:
                match = all(
                    self.metadatas[i].get(key) == value 
                    for key, value in filter_dict.items()
                )
                if not match:
                    continue
            
            similarities.append((i, similarity))
        
        # 排序取 Top-K
        similarities.sort(key=lambda x: x[1], reverse=True)
        results = [
            (self.ids[i], sim, {"text": self.texts[i], **self.metadatas[i]})
            for i, sim in similarities[:k]
        ]
        
        logger.debug(f"向量搜索：query='{query[:50]}...', k={k}, 返回{len(results)}条")
        return results
    
    def delete(self, ids: List[str]) -> bool:
        """删除文档"""
        initial_count = len(self.texts)
        
        for doc_id in ids:
            if doc_id in self.ids:
                idx = self.ids.index(doc_id)
                self.ids.pop(idx)
                self.texts.pop(idx)
                self.embeddings.pop(idx)
                self.metadatas.pop(idx)
        
        deleted_count = initial_count - len(self.texts)
        logger.info(f"内存向量库删除 {deleted_count} 个文档")
        return True
    
    def count(self) -> int:
        """返回文档数量"""
        return len(self.texts)
    
    def save(self, path: str):
        """保存到文件"""
        data = {
            "texts": self.texts,
            "embeddings": self.embeddings,
            "metadatas": self.metadatas,
            "ids": self.ids
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"内存向量库已保存：{path}")
    
    def load(self, path: str):
        """从文件加载"""
        if not os.path.exists(path):
            logger.warning(f"向量库文件不存在：{path}")
            return
        
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.texts = data.get("texts", [])
        self.embeddings = data.get("embeddings", [])
        self.metadatas = data.get("metadatas", [])
        self.ids = data.get("ids", [])
        
        logger.info(f"从 {path} 加载 {len(self.texts)} 个文档")


class ChromaVectorStore(VectorStore):
    """ChromaDB 向量存储"""
    
    def __init__(self, persist_directory: str = "./chroma_db",
                 collection_name: str = "jamin_knowledge",
                 embedding_function=None):
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        self.embedding_function = embedding_function
        self.client = None
        self.collection = None
    
    def connect(self) -> bool:
        """连接 ChromaDB"""
        try:
            import chromadb
            from chromadb.config import Settings
            
            # 持久化客户端
            self.client = chromadb.PersistentClient(path=self.persist_directory)
            
            # 获取或创建集合
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={"description": "Jamin Industrial Knowledge Base"}
            )
            
            logger.info(f"ChromaDB 就绪：{self.persist_directory}/{self.collection_name}")
            return True
            
        except ImportError:
            logger.error("缺少 chromadb 依赖：pip install chromadb")
            return False
        except Exception as e:
            logger.error(f"连接 ChromaDB 失败：{e}")
            return False
    
    def add(self, texts: List[str], embeddings: Optional[List[List[float]]] = None,
            metadatas: Optional[List[Dict[str, Any]]] = None,
            ids: Optional[List[str]] = None) -> List[str]:
        """添加文档到 ChromaDB"""
        if not self.collection:
            logger.error("ChromaDB 未连接")
            return []
        
        added_ids = []
        
        for i, text in enumerate(texts):
            doc_id = ids[i] if ids else f"doc_{len(self.collection.get()['ids']) + i}"
            metadata = metadatas[i] if metadatas else {}
            metadata["text"] = text
            metadata["created_at"] = datetime.now().isoformat()
            
            self.collection.add(
                documents=[text],
                metadatas=[metadata],
                ids=[doc_id]
            )
            added_ids.append(doc_id)
        
        logger.info(f"ChromaDB 添加 {len(texts)} 个文档")
        return added_ids
    
    def search(self, query: str, query_embedding: Optional[List[float]] = None,
               k: int = 5, filter_dict: Optional[Dict[str, Any]] = None) -> List[Tuple[str, float, Dict]]:
        """相似度搜索"""
        if not self.collection:
            logger.error("ChromaDB 未连接")
            return []
        
        try:
            results = self.collection.query(
                query_texts=[query],
                n_results=k,
                where=filter_dict
            )
            
            # 解析结果
            parsed_results = []
            if results and results['ids'] and len(results['ids'][0]) > 0:
                for i, doc_id in enumerate(results['ids'][0]):
                    distance = results['distances'][0][i] if results['distances'] else 0.0
                    metadata = results['metadatas'][0][i] if results['metadatas'] else {}
                    document = results['documents'][0][i] if results['documents'] else ""
                    
                    # ChromaDB 返回的是距离，转换为相似度
                    similarity = 1.0 - distance
                    
                    parsed_results.append((
                        doc_id,
                        similarity,
                        {"text": document, **metadata}
                    ))
            
            logger.debug(f"ChromaDB 搜索：query='{query[:50]}...', k={k}, 返回{len(parsed_results)}条")
            return parsed_results
            
        except Exception as e:
            logger.error(f"ChromaDB 搜索失败：{e}")
            return []
    
    def delete(self, ids: List[str]) -> bool:
        """删除文档"""
        if not self.collection:
            logger.error("ChromaDB 未连接")
            return False
        
        try:
            self.collection.delete(ids=ids)
            logger.info(f"ChromaDB 删除 {len(ids)} 个文档")
            return True
        except Exception as e:
            logger.error(f"ChromaDB 删除失败：{e}")
            return False
    
    def count(self) -> int:
        """返回文档数量"""
        if not self.collection:
            return 0
        return self.collection.count()


class FAISSVectorStore(VectorStore):
    """FAISS 向量存储 (适合大规模数据)"""
    
    def __init__(self, dimension: int = 768, index_type: str = "flat",
                 embedding_function=None):
        self.dimension = dimension
        self.index_type = index_type
        self.embedding_function = embedding_function
        self.index = None
        self.id_map = {}  # FAISS ID → 自定义 ID
        self.metadata_map = {}  # FAISS ID → 元数据
        self.text_map = {}  # FAISS ID → 文本
    
    def initialize(self) -> bool:
        """初始化 FAISS 索引"""
        try:
            import faiss
            
            if self.index_type == "flat":
                self.index = faiss.IndexFlatIP(self.dimension)  # 内积相似度
            elif self.index_type == "lsh":
                self.index = faiss.IndexLSH(self.dimension, 32)
            elif self.index_type == "ivf":
                quantizer = faiss.IndexFlatIP(self.dimension)
                nlist = 100  # 聚类中心数
                self.index = faiss.IndexIVFFlat(quantizer, self.dimension, nlist, faiss.METRIC_INNER_PRODUCT)
            else:
                self.index = faiss.IndexFlatIP(self.dimension)
            
            logger.info(f"FAISS 索引初始化：{self.index_type}, dimension={self.dimension}")
            return True
            
        except ImportError:
            logger.error("缺少 faiss 依赖：pip install faiss-cpu")
            return False
        except Exception as e:
            logger.error(f"FAISS 初始化失败：{e}")
            return False
    
    def add(self, texts: List[str], embeddings: Optional[List[List[float]]] = None,
            metadatas: Optional[List[Dict[str, Any]]] = None,
            ids: Optional[List[str]] = None) -> List[str]:
        """添加文档"""
        if self.index is None:
            logger.error("FAISS 索引未初始化")
            return []
        
        import faiss
        
        added_ids = []
        
        for i, text in enumerate(texts):
            custom_id = ids[i] if ids else f"doc_{len(self.id_map)}"
            emb = embeddings[i] if embeddings else self._generate_embedding(text)
            metadata = metadatas[i] if metadatas else {}
            
            # 添加到索引
            faiss_id = self.index.ntotal
            self.index.add(np.array([emb], dtype=np.float32))
            
            # 保存映射
            self.id_map[faiss_id] = custom_id
            self.metadata_map[faiss_id] = metadata
            self.text_map[faiss_id] = text
            
            added_ids.append(custom_id)
        
        logger.info(f"FAISS 添加 {len(texts)} 个文档，总计 {self.index.ntotal} 个")
        return added_ids
    
    def _generate_embedding(self, text: str) -> List[float]:
        """生成 embedding (占位符，实际应使用真实模型)"""
        # 这里应该调用真实的 embedding 模型
        # 暂时用随机向量代替
        return np.random.randn(self.dimension).astype(np.float32).tolist()
    
    def search(self, query: str, query_embedding: Optional[List[float]] = None,
               k: int = 5, filter_dict: Optional[Dict[str, Any]] = None) -> List[Tuple[str, float, Dict]]:
        """相似度搜索"""
        if self.index is None:
            logger.error("FAISS 索引未初始化")
            return []
        
        # 生成查询向量
        if query_embedding is None:
            query_embedding = self._generate_embedding(query)
        
        query_vec = np.array([query_embedding], dtype=np.float32)
        
        # 搜索
        distances, indices = self.index.search(query_vec, k)
        
        # 解析结果
        results = []
        for i, (faiss_id, distance) in enumerate(zip(indices[0], distances[0])):
            if faiss_id == -1:  # FAISS 返回 -1 表示不足 k 个
                continue
            
            custom_id = self.id_map.get(faiss_id, f"faiss_{faiss_id}")
            metadata = self.metadata_map.get(faiss_id, {})
            text = self.text_map.get(faiss_id, "")
            
            # 应用过滤
            if filter_dict:
                match = all(metadata.get(key) == value for key, value in filter_dict.items())
                if not match:
                    continue
            
            results.append((custom_id, float(distance), {"text": text, **metadata}))
        
        logger.debug(f"FAISS 搜索：query='{query[:50]}...', k={k}, 返回{len(results)}条")
        return results
    
    def delete(self, ids: List[str]) -> bool:
        """删除文档 (FAISS 不支持直接删除，需要重建索引)"""
        logger.warning("FAISS 不支持直接删除，需要重建索引")
        return False
    
    def count(self) -> int:
        """返回文档数量"""
        if self.index is None:
            return 0
        return self.index.ntotal
    
    def save(self, path: str):
        """保存索引"""
        try:
            import faiss
            
            faiss.write_index(self.index, f"{path}.index")
            
            # 保存映射
            mapping_data = {
                "id_map": {str(k): v for k, v in self.id_map.items()},
                "metadata_map": {str(k): v for k, v in self.metadata_map.items()},
                "text_map": {str(k): v for k, v in self.text_map.items()}
            }
            with open(f"{path}.mapping.json", 'w', encoding='utf-8') as f:
                json.dump(mapping_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"FAISS 索引已保存：{path}")
            
        except Exception as e:
            logger.error(f"FAISS 保存失败：{e}")
    
    def load(self, path: str):
        """加载索引"""
        try:
            import faiss
            
            if not os.path.exists(f"{path}.index"):
                logger.warning(f"FAISS 索引文件不存在：{path}")
                return
            
            self.index = faiss.read_index(f"{path}.index")
            
            # 加载映射
            mapping_path = f"{path}.mapping.json"
            if os.path.exists(mapping_path):
                with open(mapping_path, 'r', encoding='utf-8') as f:
                    mapping_data = json.load(f)
                
                self.id_map = {int(k): v for k, v in mapping_data.get("id_map", {}).items()}
                self.metadata_map = {int(k): v for k, v in mapping_data.get("metadata_map", {}).items()}
                self.text_map = {int(k): v for k, v in mapping_data.get("text_map", {}).items()}
            
            logger.info(f"FAISS 索引已加载：{path}, 文档数={self.index.ntotal}")
            
        except Exception as e:
            logger.error(f"FAISS 加载失败：{e}")


class VectorStoreManager:
    """向量存储管理器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.store: Optional[VectorStore] = None
    
    def initialize(self) -> bool:
        """根据配置初始化向量存储"""
        store_type = self.config.get("type", "memory")
        
        if store_type == "chromadb":
            self.store = ChromaVectorStore(
                persist_directory=self.config.get("persist_directory", "./chroma_db"),
                collection_name=self.config.get("collection_name", "jamin_knowledge")
            )
            return self.store.connect()
        
        elif store_type == "faiss":
            self.store = FAISSVectorStore(
                dimension=self.config.get("dimension", 768),
                index_type=self.config.get("index_type", "flat")
            )
            return self.store.initialize()
        
        elif store_type == "memory":
            self.store = MemoryVectorStore()
            return True
        
        else:
            logger.error(f"不支持的向量存储类型：{store_type}")
            return False
    
    def add_documents(self, texts: List[str], metadatas: Optional[List[Dict]] = None,
                     ids: Optional[List[str]] = None) -> List[str]:
        """添加文档"""
        if not self.store:
            logger.error("向量存储未初始化")
            return []
        return self.store.add(texts, metadatas=metadatas, ids=ids)
    
    def search(self, query: str, k: int = 5, filter_dict: Optional[Dict] = None) -> List[Tuple[str, float, Dict]]:
        """搜索"""
        if not self.store:
            logger.error("向量存储未初始化")
            return []
        return self.store.search(query, k=k, filter_dict=filter_dict)
    
    def delete(self, ids: List[str]) -> bool:
        """删除"""
        if not self.store:
            logger.error("向量存储未初始化")
            return False
        return self.store.delete(ids)
    
    def count(self) -> int:
        """文档数量"""
        if not self.store:
            return 0
        return self.store.count()


# 测试代码
if __name__ == "__main__":
    # 测试内存向量存储
    print("=== 测试 MemoryVectorStore ===")
    store = MemoryVectorStore()
    
    # 添加文档
    docs = [
        "工业物联网平台用于设备监控和预测性维护",
        "PLC 是可编程逻辑控制器，用于工业自动化",
        "SCADA 系统是数据采集与监视控制系统",
        "DCS 是分布式控制系统，用于过程控制",
        "MES 是制造执行系统，连接 ERP 和控制层"
    ]
    
    ids = store.add(docs, metadatas=[
        {"category": "platform", "source": "wiki"},
        {"category": "device", "source": "manual"},
        {"category": "system", "source": "wiki"},
        {"category": "system", "source": "manual"},
        {"category": "system", "source": "erp"}
    ])
    
    print(f"添加了 {len(ids)} 个文档")
    
    # 搜索
    queries = [
        "什么是 PLC？",
        "工业监控系统有哪些？",
        "预测性维护平台"
    ]
    
    for query in queries:
        print(f"\n查询：{query}")
        results = store.search(query, k=2)
        for doc_id, score, meta in results:
            print(f"  [{score:.3f}] {meta['text'][:60]}...")
    
    # 测试 ChromaDB (如果可用)
    print("\n=== 测试 ChromaVectorStore ===")
    try:
        chroma_store = ChromaVectorStore(persist_directory="./test_chroma")
        if chroma_store.connect():
            chroma_store.add(docs)
            results = chroma_store.search("PLC 控制器", k=2)
            for doc_id, score, meta in results:
                print(f"  [{score:.3f}] {meta['text'][:60]}...")
    except Exception as e:
        print(f"ChromaDB 测试跳过：{e}")

