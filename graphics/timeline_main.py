from sqlalchemy import create_engine, text
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd


# Database connection details
DATABASE_URI = 'postgresql://postgres:1234@localhost:5432/maybeland'  # Replace with your database URI

# Create a SQLAlchemy engine
engine = create_engine(DATABASE_URI)

# Define the SQL query
query = """
SELECT article, 
       '[' || string_agg('{ "price": ' || price || ', "date": "' || creation_time::date || '" }', ', ' ORDER BY rnk) || ']' AS prices 
FROM (
    SELECT rank() OVER (PARTITION BY article ORDER BY creation_time) AS rnk, *
    FROM flats
) subquery
GROUP BY article;
"""

# Execute the query and fetch results
with engine.connect() as connection:
    result = connection.execute(text(query))
    rows = result.fetchall()

# Save results to a list of dictionaries
results_list = []
for row in rows:
    article, prices_json = row
    if (prices_json is not None):
        prices = json.loads(prices_json)  # Convert JSON string to a Python list of dictionaries
        if (len(prices) >= 3):
            results_list.append({
                "article": article,
                "prices": prices
            })

# Print the first few results for verification
print(results_list[:5])  # Print first 5 results for debugging
num_plots = min(200, len(results_list))  # Ensure we don't exceed the available data
rows = 40  # Number of rows in the grid
cols = 5  # Number of columns in the grid

# Create a subplot grid
fig = make_subplots(rows=rows, cols=cols, subplot_titles=[f"Article {result['article']}" for result in results_list[:num_plots]])

# Add plots to the grid
for i, result in enumerate(results_list[:num_plots]):
    article = result["article"]
    prices = result["prices"]

    # Convert prices to a DataFrame for easier manipulation
    df = pd.DataFrame(prices)
    df['date'] = pd.to_datetime(df['date'])  # Convert 'date' to datetime format

    # Calculate row and column position in the grid
    row = (i // cols) + 1
    col = (i % cols) + 1

    # Add a trace for the current article
    fig.add_trace(
        go.Scatter(x=df['date'], y=df['price'], mode='lines+markers', name=f"Article {article}"),
        row=row, col=col
    )

# Update layout for better visualization
fig.update_layout(
    height=10000,  # Adjust height of the figure
    width=1400,  # Adjust width of the figure
    title_text="Price Over Time for First 20 Articles",  # Main title
    showlegend=False  # Hide legend for individual subplots
)

# Show the plot
fig.show()

fig.write_html("scrape-tms/graphics/grid_plot.html")