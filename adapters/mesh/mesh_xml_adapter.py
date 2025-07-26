"""
Adapter for MESH XML data
"""

from . import MeshAdapter
import xml.etree.ElementTree as ET
import logging

class MeshXmlAdapter(MeshAdapter):
    """Adapter for MESH XML data"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        super().__init__(file_path)
        self.logger = logging.getLogger(__name__)
        
        # Additional data structures
        self.tree_numbers = {}
        self.annotations = {}
        self.semantic_types = {}
        self.pharmacological_actions = {}
        
        if file_path:
            self.parse_data()
    
    def parse_data(self):
        """Parse data from XML file"""
        self.logger.info(f"Parsing MESH XML data from {self.file_path}")
        
        try:
            # Use iterparse to handle large XML files
            context = ET.iterparse(self.file_path, events=("start", "end"))
            
            # Track current elements
            current_descriptor = None
            current_concept = None
            current_term = None
            
            descriptor_count = 0
            
            for event, elem in context:
                # Start of a descriptor record
                if event == "start" and elem.tag == "DescriptorRecord":
                    current_descriptor = {"concepts": []}
                    descriptor_id = elem.get("DescriptorUI")
                    if descriptor_id:
                        current_descriptor["id"] = descriptor_id
                
                # End of a descriptor record
                elif event == "end" and elem.tag == "DescriptorRecord":
                    if "id" in current_descriptor:
                        descriptor_id = current_descriptor["id"]
                        self.descriptors[descriptor_id] = current_descriptor
                        
                        descriptor_count += 1
                        if descriptor_count % 1000 == 0:
                            self.logger.info(f"Processed {descriptor_count:,} descriptors")
                    
                    current_descriptor = None
                    elem.clear()  # Free memory
                
                # Descriptor name
                elif event == "end" and elem.tag == "DescriptorName" and current_descriptor:
                    name_elem = elem.find("String")
                    if name_elem is not None and name_elem.text:
                        current_descriptor["name"] = name_elem.text
                
                # Tree numbers
                elif event == "end" and elem.tag == "TreeNumber" and current_descriptor:
                    if elem.text:
                        if "tree_numbers" not in current_descriptor:
                            current_descriptor["tree_numbers"] = []
                        current_descriptor["tree_numbers"].append(elem.text)
                        
                        # Also store in separate dictionary for easy access
                        descriptor_id = current_descriptor["id"]
                        if descriptor_id not in self.tree_numbers:
                            self.tree_numbers[descriptor_id] = []
                        self.tree_numbers[descriptor_id].append(elem.text)
                
                # Start of a concept
                elif event == "start" and elem.tag == "Concept":
                    current_concept = {"terms": []}
                    concept_id = elem.get("ConceptUI")
                    if concept_id:
                        current_concept["id"] = concept_id
                
                # End of a concept
                elif event == "end" and elem.tag == "Concept":
                    if current_descriptor and "id" in current_concept:
                        concept_id = current_concept["id"]
                        self.concepts[concept_id] = current_concept
                        
                        # Link concept to descriptor
                        current_descriptor["concepts"].append(concept_id)
                        
                        # Check if preferred concept
                        if elem.get("PreferredConceptYN") == "Y":
                            current_descriptor["preferred_concept"] = concept_id
                            self.relationships.append(("descriptor_to_concept", current_descriptor["id"], concept_id, "preferred"))
                        else:
                            self.relationships.append(("descriptor_to_concept", current_descriptor["id"], concept_id, "related"))
                    
                    current_concept = None
                
                # Concept name
                elif event == "end" and elem.tag == "ConceptName" and current_concept:
                    name_elem = elem.find("String")
                    if name_elem is not None and name_elem.text:
                        current_concept["name"] = name_elem.text
                
                # Start of a term
                elif event == "start" and elem.tag == "Term":
                    current_term = {}
                    term_id = elem.get("TermUI")
                    if term_id:
                        current_term["id"] = term_id
                
                # End of a term
                elif event == "end" and elem.tag == "Term":
                    if current_concept and "id" in current_term:
                        term_id = current_term["id"]
                        self.terms[term_id] = current_term
                        
                        # Link term to concept
                        current_concept["terms"].append(term_id)
                        self.relationships.append(("concept_to_term", current_concept["id"], term_id, "related"))
                    
                    current_term = None
                
                # Term name
                elif event == "end" and elem.tag == "TermName" and current_term:
                    if elem.text:
                        current_term["name"] = elem.text
                
                # Semantic types
                elif event == "end" and elem.tag == "SemanticTypeUI" and current_descriptor:
                    if elem.text:
                        if "semantic_types" not in current_descriptor:
                            current_descriptor["semantic_types"] = []
                        current_descriptor["semantic_types"].append(elem.text)
                        
                        # Also store in separate dictionary
                        descriptor_id = current_descriptor["id"]
                        if descriptor_id not in self.semantic_types:
                            self.semantic_types[descriptor_id] = []
                        self.semantic_types[descriptor_id].append(elem.text)
                
                # Pharmacological actions
                elif event == "end" and elem.tag == "PharmacologicalAction" and current_descriptor:
                    action_elem = elem.find("DescriptorReferredTo/DescriptorUI")
                    if action_elem is not None and action_elem.text:
                        if "pharmacological_actions" not in current_descriptor:
                            current_descriptor["pharmacological_actions"] = []
                        current_descriptor["pharmacological_actions"].append(action_elem.text)
                        
                        # Also store in separate dictionary
                        descriptor_id = current_descriptor["id"]
                        if descriptor_id not in self.pharmacological_actions:
                            self.pharmacological_actions[descriptor_id] = []
                        self.pharmacological_actions[descriptor_id].append(action_elem.text)
                        
                        # Add relationship
                        self.relationships.append(("pharmacological_action", descriptor_id, action_elem.text, "has_action"))
            
            self.logger.info(f"Finished parsing {descriptor_count:,} descriptors")
            
        except Exception as e:
            self.logger.error(f"Error parsing MESH XML data: {e}")
            import traceback
            traceback.print_exc()
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        # Generate descriptor nodes
        for descriptor_id, descriptor in self.descriptors.items():
            properties = {
                "name": descriptor.get("name", ""),
                "id": descriptor_id
            }
            
            # Add tree numbers if available
            if "tree_numbers" in descriptor:
                properties["tree_numbers"] = "|".join(descriptor["tree_numbers"])
            
            # Add semantic types if available
            if "semantic_types" in descriptor:
                properties["semantic_types"] = "|".join(descriptor["semantic_types"])
            
            # Add pharmacological actions if available
            if "pharmacological_actions" in descriptor:
                properties["pharmacological_actions"] = "|".join(descriptor["pharmacological_actions"])
            
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
                    "name": concept.get("name", ""),
                    "id": concept_id
                }
            )
        
        # Generate term nodes
        for term_id, term in self.terms.items():
            yield (
                term_id,
                "mesh_term",
                {
                    "name": term.get("name", ""),
                    "id": term_id
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
            elif rel_type == "pharmacological_action":
                yield (
                    source_id,
                    target_id,
                    "mesh_pharmacological_action",
                    {
                        "relationship_type": subtype,
                        "data_source": "MESH"
                    }
                )
