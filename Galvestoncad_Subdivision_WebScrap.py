from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time

# Path to your ChromeDriver
chrome_driver_path = '/path/to/chromedriver'

# Open the website
driver = webdriver.Chrome(executable_path=chrome_driver_path)

# Load the page
url = "https://esearch.galvestoncad.org/"
driver.get(url)

# Wait for the page to load completely
time.sleep(5)

# Find the search box (subdivision search)
search_box = driver.find_element(By.ID, "subdivisionSearch")  # Adjust based on actual ID or class

# Enter subdivision name or ID you want to search
subdivision_name = "HARPER'S HIDEOUT"  # Example subdivision
search_box.send_keys(subdivision_name)

# Press Enter to perform the search
search_box.send_keys(Keys.RETURN)

# Wait for results to load
time.sleep(5)

# Parse the results
# Find the table or div that contains the results
results = driver.find_elements(By.CLASS_NAME, 'result-class')  # Update with actual class name

# Extract relevant data
subdivision_data = []
for result in results:
    # Example: Extract the text of each result
    subdivision_data.append(result.text)

# Print out the data
for data in subdivision_data:
    print(data)

# Close the browser
driver.quit()
