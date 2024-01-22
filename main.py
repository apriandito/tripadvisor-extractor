from bs4 import BeautifulSoup
import polars as pl
import os

# Create function to extract reviews from an HTML file
def extract_review(html_file):
    # Open and read the HTML file
    with open(html_file, 'r', encoding='utf-8') as file:
        content = file.read()

    # Instantiate BeautifulSoup object with the HTML content
    soup = BeautifulSoup(content, "html.parser")

    # Find all elements with '_c' class; these are the containers for individual reviews
    review_containers = soup.find_all('div', class_='_c')

    # Initialize an empty list to store review data
    reviews = []

    for container in review_containers:
        # Extract the created date of the review
        created_at_element = container.find('div', class_='biGQs _P pZUbB ncFvv osNWb')
        created_at = created_at_element.text.strip() if created_at_element else None

        # Extract the username of the reviewer
        username_element = container.find('span', class_='biGQs _P fiohW fOtGX')
        username = username_element.text.strip() if username_element else None

        # Extract the reviewer's country
        country_element = container.find('div', class_='biGQs _P pZUbB osNWb')
        country = country_element.text.strip() if country_element else None

        # Extract the review rating
        rating_element = container.find('svg', class_='UctUV d H0')
        rating = rating_element.get('aria-label').strip() if rating_element else None

        # Extract the review condition
        condition_element = container.find('div', class_='RpeCd')
        condition = condition_element.text.strip() if condition_element else None

        # Extract the review title
        title_element = container.find('span', class_='yCeTE')
        title = title_element.text.strip() if title_element else None

        # Extract the number of likes for the review
        n_like_element = container.find('span', class_='biGQs _P FwFXZ')
        n_like = n_like_element.text.strip() if n_like_element else None

        # Extract the review content
        review_element = container.find('span', class_='JguWG')
        review = review_element.text.strip() if review_element else None

        # Add the extracted data to the reviews_data list
        reviews.append({
            'created_at': created_at,
            'username': username,
            'country': country,
            'rating': rating,
            'condition': condition,
            'title': title,
            'n_like': n_like,
            'review': review})

    # Convert the list of dictionaries into a DataFrame and perform some data cleaning/processing
    df_reviews = (
        pl.DataFrame(reviews)
        .filter(pl.col('username').is_not_null())
        .with_columns(pl.col("country").str.extract(r"(\d+)\s*contributions").cast(pl.Int64).alias("contribution"))
        .with_columns(pl.col("country").str.replace_all(r"(\d+)\s*contributions", "").alias("origin"))
        .with_columns(pl.col("created_at").str.replace_all(r"Written ", "").str.to_date("%B %d, %Y"))
        .with_columns(pl.col("rating").str.replace_all(r".0 of 5 bubbles", "").cast(pl.Int64))
        .with_columns(pl.col("n_like").cast(pl.Int64))
        .drop('country')
    )

    # Return DataFrame
    return df_reviews


# Function to list all HTML files in a directory
def list_html_files(data_dir):
    # Initialize an empty list to store html file
    html_files = []

    # Loop through data_dir recursively using os.walk
    for root, dirs, files in os.walk(data_dir):
        for file in files:

            # Check if it's a normal file (not dir)
            if os.path.isfile(os.path.join(root, file)):

                # Take only files with .html extension
                if file.lower().endswith('.html'):
                    # Generate the full file path
                    file_path = os.path.join(root, file)

                    # Add the file path to the list
                    html_files.append(file_path)

    # Return the list of files
    return html_files


# Get a list of HTML files in the "data/html" directory
list_files = list_html_files("data/html")

# Extract review data from each HTML file and concatenate the resulting DataFrames
df_review = pl.concat(list(map(extract_review, list_files)))

# Write the DataFrame to a CSV file
df_review.write_csv("data/review.csv")
