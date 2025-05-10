import streamlit as st
import requests
import pandas as pd

def generate_sql(query, tables):
    try:
        response = requests.post(
            "http://0.0.0.0:8000/generate_sql",
            json={"query": query, "tables": tables},
            headers={"Content-Type": "application/json"},
            timeout=300
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response is not None:
            error_details = e.response.json()
            return {"error": f"Backend Error: {error_details.get('detail', str(e))}"}
        return {"error": f"Connection Error: {str(e)}"}

def get_database_schema():
    try:
        response = requests.post(
            "http://0.0.0.0:8000/get_database_schema",
            json={},
            headers={"Content-Type": "application/json"},
            timeout=300
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response is not None:
            error_details = e.response.json()
            return {"error": f"Backend Error: {error_details.get('detail', str(e))}"}
        return {"error": f"Connection Error: {str(e)}"}
    
def main():
    st.set_page_config(page_title="Text-2-SQL", layout="wide", menu_items={})
    st.title("Text-2-SQL")

    st.markdown("""
    <style>
        .reportview-container {
            margin-top: -2em;
        }
        #MainMenu {visibility: hidden;}
        .stDeployButton {display:none;}
        footer {visibility: hidden;}
        #stDecoration {display:none;}
        .stAppToolbar {display:none;}
                
        .block-container
        {
            padding-top: 1rem;
            padding-bottom: 1rem;
            margin-top: 1rem;
        }
    </style>
""", unsafe_allow_html=True)

    filter_table_checkboxes = []
    with st.sidebar:
        st.header("Database Schema", divider="gray")
        st.subheader("Check all tables relevant to your query.")
        response = get_database_schema()
        for t in response["schema"].split("\n\n"):
            table_name = t[t.find(" ") + 1:t.find("\n")]
            t = t.replace("\n", "  \n")
            t = t.replace("Table", "**Table**")
            t = t.replace("Columns", "**Columns**")
            t = t.replace("Primary Key", "**Primary Key**")
            t = t.replace("Foreign Key", "**Foreign Key**")
            check = st.checkbox(t)
            filter_table_checkboxes.append((check, table_name))

    try:
        question = st.text_area("Enter your question in natural language:", height=100)
        
        if st.button("Generate SQL"):
            if not question:
                st.warning("Please enter a question")
                return
            
            
            with st.spinner("Generating PostgreSQL query..."):
                try:
                    filtered_tables = []
                    for table, name in filter_table_checkboxes:
                        if table:
                            filtered_tables.append(name)
                    if not filtered_tables:
                        st.warning("Please select relevant tables")
                        return
                    
                    response = generate_sql(question, filtered_tables)
                    if response.get('sql_query'):
                        st.info(response.get('sql_query'))
                        if response.get('execute_query'):
                            execute_query = response.get('execute_query')
                            columns = execute_query["columns"]
                            data = execute_query["data"]
                            if columns and data:
                                df = pd.DataFrame(data, columns=columns)
                                st.dataframe(df, hide_index=True)
                    else:
                        st.info(response)
                        
                except Exception as e:
                    st.error(f"Error generating SQL: {str(e)}")
        
    except Exception as e:
        st.error(f"Application error: {str(e)}")

if __name__ == "__main__":
    main()