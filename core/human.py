# core/human.py
# V26-C Human Smart Mode
import time, random, math
from selenium.webdriver.common.action_chains import ActionChains

def wait_smart(min_s=0.25, max_s=1.2):
    time.sleep(random.uniform(min_s, max_s))

def scroll_smart(driver):
    dist = random.randint(150, 320)
    smooth_steps = random.randint(6, 12)

    for _ in range(smooth_steps):
        driver.execute_script(f"window.scrollBy(0,{dist/smooth_steps});")
        time.sleep(random.uniform(0.02, 0.06))

def move_bezier(driver, element):
    actions = ActionChains(driver)
    actions.move_to_element(element).perform()

    steps = 10
    for i in range(steps):
        t = i / (steps - 1)
        off_x = int((1 - t) * random.randint(-60, 60))
        off_y = int((1 - t) * random.randint(-40, 40))
        ActionChains(driver) \
            .move_to_element_with_offset(element, off_x, off_y) \
            .pause(random.uniform(0.03, 0.08)) \
            .perform()

def human_click_smart(driver, element):
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
    wait_smart()

    pattern = random.choice(["bezier", "direct"])

    if pattern == "bezier":
        move_bezier(driver, element)
    else:
        ActionChains(driver).move_to_element(element).pause(0.15).perform()

    wait_smart(0.15, 0.4)
    ActionChains(driver).click(element).perform()

def human_type_smart(element, text):
    element.clear()
    time.sleep(0.25)

    element.click()
    wait_smart(0.2, 0.4)

    for ch in text:
        element.send_keys(ch)
        time.sleep(random.uniform(0.06, 0.17))
