import random
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

# URL of the web page
URL = "http://localhost:5000"  # Change to your web app's URL

# Define curated list of products and other random values
PRODUCTS = [
    "Wireless Mouse", "Keyboard", "Monitor", "Laptop", "Headphones",
    "Webcam", "External Hard Drive", "Smartphone", "Tablet", "Charger",
    "Smartwatch", "Gaming Console", "Desk Lamp", "USB Cable", "Speakers",
    "Router", "Projector", "Graphic Tablet", "Microphone", "Power Bank"
]
BRANCHES = ["New York", "Boston", "Los Angeles", "Chicago", "Houston"]
STATES = ["New York", "California", "Texas", "Illinois", "Massachusetts"]
CITIES = ["NYC", "Boston", "LA", "Chicago", "Houston"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "UPI"]

# Dynamic shopping experience sentence templates
SHOPPING_EXPERIENCE_TEMPLATES = [
    "The service at the {branch} branch was {adjective}.",
    "I found the {product} to be {adjective}, but the checkout process was {adjective}.",
    "Overall, my experience at the {branch} branch was {adjective}.",
    "The {product} quality was {adjective}, but the staff was {adjective}.",
    "Shopping at the {branch} branch was {adjective}, especially the {product}.",
    "I really {verb} the {product} I bought from the {branch} branch."
]
ADJECTIVES = ["excellent", "average", "terrible", "satisfactory", "outstanding", "poor"]
VERBS = ["loved", "hated", "enjoyed", "appreciated"]

# Set up Selenium WebDriver
driver = webdriver.Chrome()  # Ensure chromedriver is installed and in PATH
driver.get(URL)

def generate_shopping_experience(branch, product):
    """Generate a dynamic shopping experience sentence."""
    template = random.choice(SHOPPING_EXPERIENCE_TEMPLATES)
    adjective = random.choice(ADJECTIVES)
    verb = random.choice(VERBS)
    return template.format(branch=branch, product=product, adjective=adjective, verb=verb)

def fill_and_submit_form():
    # Randomly select values for the form fields
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 10)
    branch = random.choice(BRANCHES)
    state = random.choice(STATES)
    city = random.choice(CITIES)
    payment_method = random.choice(PAYMENT_METHODS)
    shopping_experience = generate_shopping_experience(branch, product)
    total_amount = round(random.uniform(10.0, 500.0), 2)

    # Fill the form
    Select(driver.find_element(By.ID, "product_name")).select_by_visible_text(product)
    driver.find_element(By.ID, "quantity").clear()
    driver.find_element(By.ID, "quantity").send_keys(quantity)
    driver.find_element(By.ID, "state").clear()
    driver.find_element(By.ID, "state").send_keys(state)
    driver.find_element(By.ID, "city").clear()
    driver.find_element(By.ID, "city").send_keys(city)
    driver.find_element(By.ID, "branch").clear()
    driver.find_element(By.ID, "branch").send_keys(branch)
    driver.find_element(By.ID, "shopping_experience").clear()
    driver.find_element(By.ID, "shopping_experience").send_keys(shopping_experience)
    Select(driver.find_element(By.ID, "payment_method")).select_by_visible_text(payment_method)
    driver.find_element(By.ID, "total_amount").clear()
    driver.find_element(By.ID, "total_amount").send_keys(total_amount)

    # Submit the form
    driver.find_element(By.TAG_NAME, "button").click()

    print(f"Submitted: {product}, {quantity}, {state}, {city}, {branch}, {payment_method}, {shopping_experience}, {total_amount}")

# Simulate sales with random intervals
try:
    while True:
        fill_and_submit_form()
        time.sleep(random.randint(5, 15))  # Wait for 5-15 seconds before submitting the next form
except KeyboardInterrupt:
    print("Simulation stopped.")
finally:
    driver.quit()
