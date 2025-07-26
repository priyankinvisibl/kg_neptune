"""
Adapter for MESH NT data
"""

from . import MeshAdapter
import re
import logging

class MeshAdapterComprehensive(MeshAdapter):
    """Comprehensive adapter for MESH NT data"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        super().__init__(file_path)
        self.logger = logging.getLogger(__name__)
        
        # Additional data structures
        self.tree_numbers = {}
        self.annotations = {}
        self.semantic_types = {}
        
        if file_path:
            self.parse_data()
    
    def parse_data(self):
        """Parse data from NT file"""
        self.logger.info(f"Parsing MESH NT data from {self.file_path}")
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                line_count = 0
                for line in f:
                    line_count += 1
                    if line_count % 100000 == 0:
                        self.logger.info(f"Processed {line_count:,} lines")
                    
                    # Parse NT triple
                    self._parse_triple(line)
            
            self.logger.info(f"Finished parsing {line_count:,} lines")
            
        except Exception as e:
            self.logger.error(f"Error parsing MESH NT data: {e}")
            import traceback
            traceback.print_exc()
    
    def _parse_triple(self, line):
        """Parse an NT triple"""
        # Basic NT triple pattern
        pattern = r'<([^>]+)>\s+<([^>]+)>\s+(.+)\s+\.'
        match = re.match(pattern, line)
        
        if not match:
            return
        
        subject, predicate, obj = match.groups()
        
        # Process based on predicate
        if "rdf-schema#label" in predicate:
            # Extract label
            label_match = re.match(r'"([^"]+)"', obj)
            if label_match:
                label = label_match.group(1)
                
                # Determine entity type from subject URI
                if "/descriptor/" in subject:
                    descriptor_id = subject.split("/")[-1]
                    self.descriptors[descriptor_id] = {"label": label, "uri": subject}
                elif "/concept/" in subject:
                    concept_id = subject.split("/")[-1]
                    self.concepts[concept_id] = {"label": label, "uri": subject}
                elif "/term/" in subject:
                    term_id = subject.split("/")[-1]
                    self.terms[term_id] = {"label": label, "uri": subject}
                elif "/qualifier/" in subject:
                    qualifier_id = subject.split("/")[-1]
                    self.qualifiers[qualifier_id] = {"label": label, "uri": subject}
        
        elif "preferredConcept" in predicate:
            # Link descriptor to preferred concept
            if "/descriptor/" in subject and "/concept/" in obj:
                descriptor_id = subject.split("/")[-1]
                concept_id = obj.split("/")[-1].replace(">", "")
                
                if descriptor_id in self.descriptors:
                    self.descriptors[descriptor_id]["preferred_concept"] = concept_id
                    self.relationships.append(("descriptor_to_concept", descriptor_id, concept_id, "preferred"))
        
        elif "concept" in predicate and "preferred" not in predicate:
            # Link descriptor to concept
            if "/descriptor/" in subject and "/concept/" in obj:
                descriptor_id = subject.split("/")[-1]
                concept_id = obj.split("/")[-1].replace(">", "")
                
                self.relationships.append(("descriptor_to_concept", descriptor_id, concept_id, "related"))
        
        elif "term" in predicate:
            # Link concept to term
            if "/concept/" in subject and "/term/" in obj:
                concept_id = subject.split("/")[-1]
                term_id = obj.split("/")[-1].replace(">", "")
                
                self.relationships.append(("concept_to_term", concept_id, term_id, "related"))
        
        elif "treeNumber" in predicate:
            # Extract tree number
            tree_match = re.match(r'"([^"]+)"', obj)
            if tree_match and "/descriptor/" in subject:
                tree_number = tree_match.group(1)
                descriptor_id = subject.split("/")[-1]
                
                if descriptor_id not in self.tree_numbers:
                    self.tree_numbers[descriptor_id] = []
                
                self.tree_numbers[descriptor_id].append(tree_number)
        
        elif "annotation" in predicate:
            # Extract annotation
            annotation_match = re.match(r'"([^"]+)"', obj)
            if annotation_match:
                annotation = annotation_match.group(1)
                entity_id = subject.split("/")[-1]
                
                if entity_id not in self.annotations:
                    self.annotations[entity_id] = []
                
                self.annotations[entity_id].append(annotation)
        
        elif "semanticType" in predicate:
            # Extract semantic type
            semantic_match = re.match(r'"([^"]+)"', obj)
            if semantic_match and "/descriptor/" in subject:
                semantic_type = semantic_match.group(1)
                descriptor_id = subject.split("/")[-1]
                
                if descriptor_id not in self.semantic_types:
                    self.semantic_types[descriptor_id] = []
                
                self.semantic_types[descriptor_id].append(semantic_type)
        
        elif "broader" in predicate or "narrower" in predicate:
            # Handle hierarchical relationships
            if "/descriptor/" in subject and "/descriptor/" in obj:
                source_id = subject.split("/")[-1]
                target_id = obj.split("/")[-1].replace(">", "")
                rel_type = "broader" if "broader" in predicate else "narrower"
                
                self.relationships.append(("descriptor_hierarchy", source_id, target_id, rel_type))
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        # Generate descriptor nodes
        for descriptor_id, descriptor in self.descriptors.items():
            properties = {
                "name": descriptor.get("label", ""),
                "id": descriptor_id,
                "uri": descriptor.get("uri", "")
            }
            
            # Add tree numbers if available
            if descriptor_id in self.tree_numbers:
                properties["tree_numbers"] = "|".join(self.tree_numbers[descriptor_id])
            
            # Add annotations if available
            if descriptor_id in self.annotations:
                properties["annotations"] = "|".join(self.annotations[descriptor_id])
            
            # Add semantic types if available
            if descriptor_id in self.semantic_types:
                properties["semantic_types"] = "|".join(self.semantic_types[descriptor_id])
            
            yield (
                descriptor_id,
                "mesh_descriptor",
                properties
            )
        
        # Generate concept nodes
        for concept_id, concept in self.concepts.items():
            yield (
                concept_id,
                "mesh_concept",
                {
                    "name": concept.get("label", ""),
                    "id": concept_id,
                    "uri": concept.get("uri", "")
                }
            )
        
        # Generate term nodes
        for term_id, term in self.terms.items():
            yield (
                term_id,
                "mesh_term",
                {
                    "name": term.get("label", ""),
                    "id": term_id,
                    "uri": term.get("uri", "")
                }
            )
        
        # Generate qualifier nodes
        for qualifier_id, qualifier in self.qualifiers.items():
            yield (
                qualifier_id,
                "mesh_qualifier",
                {
                    "name": qualifier.get("label", ""),
                    "id": qualifier_id,
                    "uri": qualifier.get("uri", "")
                }
            )
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        # Generate edges from relationships
        for rel_type, source_id, target_id, subtype in self.relationships:
            if rel_type == "descriptor_to_concept":
                yield (
                    source_id,
                    target_id,
                    "mesh_descriptor_to_concept",
                    {
                        "relationship_type": subtype,
                        "data_source": "MESH"
                    }
                )
            elif rel_type == "concept_to_term":
                yield (
                    source_id,
                    target_id,
                    "mesh_concept_to_term",
                    {
                        "relationship_type": subtype,
                        "data_source": "MESH"
                    }
                )
            elif rel_type == "descriptor_hierarchy":
                yield (
                    source_id,
                    target_id,
                    "mesh_descriptor_hierarchy",
                    {
                        "relationship_type": subtype,
                        "data_source": "MESH"
                    }
                )
