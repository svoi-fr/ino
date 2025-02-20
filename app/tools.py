import json
from mistralai import Mistral
from config import MISTRAL_API_KEY

tool_defs = {
    "id_url_language": {
        "description": "Provided a list of URLs, try to deduce a regex pattern for language",
        "parameters": {
            "patterns": [
                {
                    "code": ("Language code", ["en", "fr", "de"]),
                    "pattern": "Regex pattern",
                }
            ],
        },
    },
    "id_website": {
        "description": "Idenitfy website parameters",
        "parameters": {
            "country": "Country name in english",
            "country_code": "Country code",
            "description": "Website description",
            "required": ["country", "country_code", "description"],
        },
    },
    "classify_page": {
        "description": "Classify a page based on content",
        "parameters": {
            "summary": "Page summary",
            "type": (
                "Page type",
                [
                    "guide",
                    "org_description",
                    "news",
                    "discussion",
                    "experience",
                    "other",
                ],
            ),
            "category": (
                "Page category",
                [
                    "immigration",
                    "legal",
                    "daily_life",
                    "health",
                    "education",
                    "jobs",
                    "other",
                ],
            ),
            "required": ["summary", "type", "category"],
        },
    },
    "extract_org_info": {
        "description": "Extract organization information from a page",
        "parameters": {
            "organizations": [
                {
                    "name": "Organization name",
                    "description": "Organization description",
                    "type": ("Organization type", ["office", "online_only"]),
                    "url": "Organization URL",
                    "contacts": [
                        {
                            "address": "Contact address. For online_only types, must mention country",
                            "phone": None,
                            "email": None,
                            "website": None,
                            "required": ["address"],
                        }
                    ],
                }
            ]
        },
    },
}

def process_properties(properties_def):
    """Recursively process property definitions"""
    if properties_def is None:
        return {"type": "string"}
        
    if isinstance(properties_def, str):
        return {
            "type": "string",
            "description": properties_def
        }
        
    if isinstance(properties_def, tuple):
        result = {
            "type": "string",
            "description": properties_def[0]
        }
        if len(properties_def) > 1:
          if isinstance(properties_def[1], list):
            result["enum"] = properties_def[1]
          if isinstance(properties_def[1], int) or isinstance(properties_def[1], float):
            result["type"] = "number"
          if isinstance(properties_def[1], bool):
            result["type"] = "boolean"
        return result
        
    if isinstance(properties_def, list):
        item_props, item_required = process_object_properties(properties_def[0])
        result = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": item_props
            }
        }
        if item_required:
            result["items"]["required"] = item_required
        return result
        
    if isinstance(properties_def, dict):
        if "type" in properties_def:
            return properties_def
        props, required = process_object_properties(properties_def)
        result = {
            "type": "object",
            "properties": props
        }
        if required:
            result["required"] = required
        return result
        
    return {"type": "string"}

def process_object_properties(obj):
    """Process object properties and handle required fields"""
    properties = {}
    required = []
    
    for key, value in obj.items():
        if key == "required":
            required = value
            continue
            
        properties[key] = process_properties(value)
        
    return properties, required

def tool_definition(tool_dict : dict):
    result = []
    
    for key, value in tool_dict.items():
        tool = {
            "type": "function",
            "function": {
                "name": key,
                "description": value["description"],
                "parameters": {
                    "type": "object",
                    "properties": {}
                }
            }
        }

        if "parameters" in value:
            properties, required = process_object_properties(value["parameters"])
            tool["function"]["parameters"]["properties"] = properties
            if required:
                tool["function"]["parameters"]["required"] = required

        result.append(tool)

    return result

def quicktool(tool_name, tool_description, **kwargs):
  keys = list(kwargs.keys())
  kwargs["required"] = keys
  definition = {
      tool_name: {
        "description": tool_description,
        "parameters": kwargs,
      }
  }
  return tool_definition(definition)[0]

def simple_tool_call(input_data, function_description, **kwargs):
  tool = quicktool("function", function_description, **kwargs)
  print(json.dumps(tool, indent=2))
  if isinstance(input_data, str):
      input_data = [{"role": "user", "content": input_data}]
  return llm_tool_execute(input_data, [tool])[0]

def tool_call(input_data, tools):
  if isinstance(input_data, str):
    input_data = [{"role": "user", "content": input_data}]
  if isinstance(tools, str):
    tools = [tools]
  if isinstance(tools, list[str]):
    defs = {tool: tool_defs[tool] for tool in tools}
    tools = tool_definition(defs)
  return llm_tool_execute(input_data, tools)

def llm_tool_execute(messages, tools, tool_choice="any"):
  client = Mistral(MISTRAL_API_KEY)
  
  response = client.chat.complete(
      model="mistral-small-latest", 
      messages=messages,
      tools=tools,
      tool_choice=tool_choice  # Force tool use
  )
  
  # Extract tool call 
  tool_call = response.choices[0].message.tool_calls[0]
  
  # Return tuple of id and parsed args
  return (
      json.loads(tool_call.function.arguments),
      tool_call.function.name,
      tool_call.id
  )


if __name__ == "__main__":
    res = simple_tool_call("Hello, I am looking for a job in Paris", "Identify message parameters", language="Message language code", intent="Message intent")
    print(res)
