from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer
from sql import get_database_schema, execute_sql_query

model_name = "Qwen/Qwen2.5-Coder-7B-Instruct-GPTQ-Int4"
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    torch_dtype="auto",
    device_map="auto"
)
tokenizer = AutoTokenizer.from_pretrained(model_name)


app = FastAPI(
    title="Text-2-SQL API",
    description="API for converting natural language text to SQL and executing queries",
    version="1.0"
)

class QueryRequest(BaseModel):
    query: str
    tables: list[str]

class SchemaRequest(BaseModel):
    None


def create_prompt(question, filtered_tables):
    schema = get_database_schema(filtered_tables)
    return f"""You are a data science expert. Below, you are provided with a database schema and a natural language question. Your task is to understand the schema and generate a valid PostgreSQL query to answer the question.
    
Database Schema:
{schema}

Question:
{question}

Instructions:
- Make sure you only output the information that is asked in the question. If the question asks for a specific column, make sure to only include that column in the SELECT clause, nothing more.
- The generated query should return all of the information asked in the question without any missing or extra information.
- Before generating the final SQL query, please think through the steps of how to write the query. Do all the explanation before generating the final query.
- Make sure to check the datatypes of the columns. For Example: if Date column has text datatype, do not use date functions on it, use string functions.
- If the question has no relevance to the database provided, do not answer with any SQL query.

Take a deep breath and think step by step to find the correct SQL query.
"""

@app.post("/generate_sql")
async def get_sql(request: QueryRequest):
    """Endpoint for generating SQL from natural language"""
    try:
        question = request.query
        tables = request.tables

        prompt = create_prompt(question, tables)
        messages = [
            {"role": "system", "content": "You are Qwen, created by Alibaba Cloud. You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ]

        text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True
        )
        model_inputs = tokenizer([text], return_tensors="pt").to(model.device)

        generated_ids = model.generate(
            **model_inputs,
            max_new_tokens=512
        )
        generated_ids = [
            output_ids[len(input_ids):] for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)
        ]

        response = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]

        execute_query = None
        if "```sql" in response:
            sql_query = response[response.rfind("```sql\n") + 7:response.rfind("```")]
            execute_query = execute_sql_query(sql_query)

        return {
            "status": "success",
            "sql_query": response,
            "execute_query": execute_query
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}\n")
        raise HTTPException(status_code=500, detail="Internal server error")
    

@app.post("/get_database_schema")
async def get_schema(request: SchemaRequest):
    return {
        "status": "success",
        "schema": get_database_schema()
    }