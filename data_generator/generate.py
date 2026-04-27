import pandas as pd
import random
import os
from faker import Faker
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()

fake = Faker('en_AU')

# ── Pipeline config ───────────────────────────────────────
NUM_TRANSACTIONS = int(os.getenv("NUM_TRANSACTIONS", 500))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data_generator/output")

# ── Master data config ────────────────────────────────────
NUM_PRODUCTS  = 50
NUM_STORES    = 10
NUM_CUSTOMERS = 200

# ── FMCG categories and brands ────────────────────────────
CATEGORIES = {
    "Beverages": {
        "brands":    ["Coca Cola", "Pepsi", "Schweppes",
                      "Red Bull", "Bundaberg"],
        "products":  ["Cola 375ml", "Diet Cola 375ml", "Lemonade 600ml",
                      "Orange Juice 1L", "Energy Drink 250ml"],
        "price_range": (1.50, 5.00)
    },
    "Snacks": {
        "brands":    ["Smiths", "Doritos", "Pringles",
                      "Shapes", "Grain Waves"],
        "products":  ["Chips Original 150g", "Corn Chips 200g",
                      "Rice Crackers 100g", "Popcorn 80g", "Pretzels 150g"],
        "price_range": (2.00, 6.00)
    },
    "Dairy": {
        "brands":    ["Pauls", "Dairy Farmers", "Devondale",
                      "Bega", "Mainland"],
        "products":  ["Full Cream Milk 2L", "Skim Milk 2L",
                      "Cheese Slices 250g", "Yoghurt 500g", "Butter 250g"],
        "price_range": (2.50, 8.00)
    },
    "Bakery": {
        "brands":    ["Tip Top", "Wonder White", "Helgas",
                      "Bakers Delight", "Abbott"],
        "products":  ["White Bread 700g", "Wholemeal Bread 700g",
                      "Sourdough 600g", "Multigrain Rolls 6pk",
                      "Crumpets 6pk"],
        "price_range": (3.00, 7.00)
    },
    "Personal Care": {
        "brands":    ["Dove", "Palmolive", "Head Shoulders",
                      "Colgate", "Gillette"],
        "products":  ["Shampoo 400ml", "Conditioner 400ml",
                      "Body Wash 500ml", "Toothpaste 110g",
                      "Deodorant 150ml"],
        "price_range": (4.00, 12.00)
    },
    "Cleaning": {
        "brands":    ["Ajax", "Domestos", "Morning Fresh",
                      "Finish", "Vanish"],
        "products":  ["Dishwashing Liquid 500ml", "Bleach 1L",
                      "Spray Cleaner 500ml", "Dishwasher Tablets 30pk",
                      "Stain Remover 500ml"],
        "price_range": (3.50, 14.00)
    }
}

STATES = {
    "NSW": {"regions": ["Metro", "Regional"], "weight": 0.35},
    "VIC": {"regions": ["Metro", "Regional"], "weight": 0.25},
    "QLD": {"regions": ["Metro", "Regional"], "weight": 0.20},
    "WA":  {"regions": ["Metro", "Regional"], "weight": 0.10},
    "SA":  {"regions": ["Metro", "Regional"], "weight": 0.10}
}

STORE_TYPES     = ["Supermarket", "Convenience", "Wholesale", "Online"]
AGE_GROUPS      = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
LOYALTY_TIERS   = ["Bronze", "Silver", "Gold", "Platinum"]
DISCOUNT_OPTIONS = [0.00, 0.00, 0.00, 0.05, 0.10, 0.15, 0.20]


# ── Validation ────────────────────────────────────────────
def validate_run_date(run_date: str) -> datetime:
    """
    Validate RUN_DATE format early — fail fast with a clear message
    rather than a confusing strptime error deep in the stack.
    """
    try:
        return datetime.strptime(run_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"Invalid RUN_DATE format: '{run_date}'. "
            f"Expected YYYY-MM-DD e.g. '2024-01-15'"
        )


# ── Master data generators ────────────────────────────────
def generate_products(rng: random.Random) -> pd.DataFrame:
    """
    Generate a fixed master list of 50 products.
    Uses a dedicated Random instance so adding fields to other
    generators never shifts which products are picked.
    """
    products = []
    for i in range(1, NUM_PRODUCTS + 1):
        category = rng.choice(list(CATEGORIES.keys()))
        cat_data = CATEGORIES[category]
        brand    = rng.choice(cat_data["brands"])
        product  = rng.choice(cat_data["products"])
        min_price, max_price = cat_data["price_range"]
        unit_cost  = round(rng.uniform(min_price * 0.4, min_price * 0.7), 2)
        unit_price = round(rng.uniform(min_price, max_price), 2)

        products.append({
            "product_id":   f"PRD-{i:03d}",
            "product_name": f"{brand} {product}",
            "category":     category,
            "brand":        brand,
            "supplier":     f"{brand} Australia",
            "unit_cost":    unit_cost,
            "unit_price":   unit_price
        })

    return pd.DataFrame(products)


def generate_stores(rng: random.Random) -> pd.DataFrame:
    """
    Generate a fixed master list of 10 stores across Australian states.
    Weighted by state population.
    """
    stores     = []
    state_list = list(STATES.keys())
    state_weights = [STATES[s]["weight"] for s in state_list]

    for i in range(1, NUM_STORES + 1):
        state      = rng.choices(state_list, weights=state_weights, k=1)[0]
        region     = rng.choice(STATES[state]["regions"])
        store_type = rng.choice(STORE_TYPES)
        city       = fake.city()

        stores.append({
            "store_id":   f"STR-{i:03d}",
            "store_name": f"{city} {store_type}",
            "state":      state,
            "region":     region,
            "store_type": store_type,
            "city":       city
        })

    return pd.DataFrame(stores)


def generate_customers(rng: random.Random) -> pd.DataFrame:
    """
    Generate a fixed master list of 200 customers.
    Age groups and loyalty tiers weighted to reflect realistic
    distribution.
    """
    customers      = []
    age_weights     = [0.10, 0.25, 0.25, 0.20, 0.12, 0.08]
    loyalty_weights = [0.40, 0.30, 0.20, 0.10]
    state_list      = list(STATES.keys())
    state_weights   = [STATES[s]["weight"] for s in state_list]

    for i in range(1, NUM_CUSTOMERS + 1):
        customers.append({
            "customer_id":  f"CST-{i:04d}",
            "age_group":    rng.choices(
                                AGE_GROUPS,
                                weights=age_weights, k=1)[0],
            "loyalty_tier": rng.choices(
                                LOYALTY_TIERS,
                                weights=loyalty_weights, k=1)[0],
            "state":        rng.choices(
                                state_list,
                                weights=state_weights, k=1)[0]
        })

    return pd.DataFrame(customers)


# ── SCD change simulation ─────────────────────────────────
def simulate_dimension_changes(
    products_df: pd.DataFrame,
    stores_df:   pd.DataFrame,
    run_dt:      datetime,
    rng:         random.Random
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Simulate realistic dimension changes to trigger SCD Type 2.
    Only fires on Mondays — realistic weekly price review cycle.
    Uses dedicated rng instance so changes are isolated from
    fact generation randomness.
    """
    if run_dt.weekday() != 0:
        return products_df, stores_df

    # Change price on 2 random products
    change_indices = rng.sample(range(len(products_df)), 2)
    for idx in change_indices:
        category = products_df.at[idx, "category"]
        min_price, max_price = CATEGORIES[category]["price_range"]
        new_price = round(rng.uniform(min_price, max_price), 2)
        new_cost  = round(new_price * rng.uniform(0.4, 0.7), 2)
        products_df.at[idx, "unit_price"] = new_price
        products_df.at[idx, "unit_cost"]  = new_cost

    # Change region on 1 random store — exclude current region so
    # the value always differs and dbt snapshot detects the change.
    change_store    = rng.randint(0, len(stores_df) - 1)
    current_state   = stores_df.at[change_store, "state"]
    current_region  = stores_df.at[change_store, "region"]
    other_regions   = [r for r in STATES[current_state]["regions"]
                       if r != current_region]
    new_region      = rng.choice(other_regions)
    stores_df.at[change_store, "region"] = new_region

    return products_df, stores_df


# ── Fact table generator ──────────────────────────────────
def generate_fact_sales(
    products_df:   pd.DataFrame,
    stores_df:     pd.DataFrame,
    customers_df:  pd.DataFrame,
    run_date:      str,
    run_timestamp: str
) -> pd.DataFrame:
    """
    Generate daily sales transactions.
    Accepts run_date and run_timestamp explicitly — no global reads.
    Uses system random (non-seeded) so facts differ every run.
    """
    transactions    = []
    product_records  = products_df.to_dict("records")
    store_records    = stores_df.to_dict("records")
    customer_records = customers_df.to_dict("records")

    for i in range(1, NUM_TRANSACTIONS + 1):
        product    = random.choice(product_records)
        store      = random.choice(store_records)
        customer   = random.choice(customer_records)
        quantity   = random.choices(
            [1, 2, 3, 4, 5, 6, 10],
            weights=[0.35, 0.25, 0.20, 0.10, 0.05, 0.03, 0.02],
            k=1
        )[0]
        discount_pct = random.choice(DISCOUNT_OPTIONS)
        unit_price   = product["unit_price"]
        total_amount = round(
            quantity * unit_price * (1 - discount_pct), 2
        )

        transactions.append({
            "transaction_id":   f"TXN-{run_date.replace('-', '')}-{i:04d}",
            "transaction_date": run_date,
            "product_id":       product["product_id"],
            "store_id":         store["store_id"],
            "customer_id":      customer["customer_id"],
            "quantity":         quantity,
            "unit_price":       unit_price,
            "discount_pct":     discount_pct,
            "total_amount":     total_amount,
            "created_at":       run_timestamp
        })

    return pd.DataFrame(transactions)


# ── CSV helper ────────────────────────────────────────────
def save_csv(df: pd.DataFrame, filename: str) -> str:
    """
    Save DataFrame to CSV. Raises exception on failure so the
    caller knows the file was not written — never returns a
    filepath implying success when the write failed.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    try:
        df.to_csv(filepath, index=False)
        print(f"  Saved {len(df)} rows -> {filepath}")
        return filepath
    except Exception as e:
        raise RuntimeError(
            f"Failed to write {filename}: {e}"
        )


# ── Main ──────────────────────────────────────────────────
def main(run_date: str = None, run_timestamp: str = None) -> dict:
    """
    Main orchestration function — generates all datasets for one run.

    Parameters
    ----------
    run_date      : YYYY-MM-DD string. Defaults to today.
                    Pass explicitly for backfill.
    run_timestamp : YYYY-MM-DD HH:MM:SS string. Defaults to now.
                    Stamped on every fact row as created_at watermark.

    Returns
    -------
    dict of {table_name: filepath} for Lambda to upload to S3.
    """
    # ── Resolve run identifiers ───────────────────────────
    run_date = (run_date or
                os.getenv("RUN_DATE", "").strip() or
                date.today().strftime("%Y-%m-%d"))

    run_timestamp = (run_timestamp or
                     datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # ── Validate early ────────────────────────────────────
    run_dt = validate_run_date(run_date)

    print("Starting data generation")
    print(f"  Run date:      {run_date}")
    print(f"  Run timestamp: {run_timestamp}")
    print(f"  Transactions:  {NUM_TRANSACTIONS}")

    # ── Dedicated seeded rng for dimensions ───────────────
    # Isolated from global random state — adding fields to any
    # generator never shifts other generators' outputs.
    dim_rng = random.Random(42)
    scd_rng = random.Random(42)

    # ── Step 1: Generate master dimension data ────────────
    print("\n[1/4] Generating master data...")
    products_df  = generate_products(dim_rng)
    stores_df    = generate_stores(dim_rng)
    customers_df = generate_customers(dim_rng)
    print(f"  Products:  {len(products_df)}")
    print(f"  Stores:    {len(stores_df)}")
    print(f"  Customers: {len(customers_df)}")

    # ── Step 2: Simulate SCD changes ─────────────────────
    print("\n[2/4] Checking for dimension changes...")
    products_df, stores_df = simulate_dimension_changes(
        products_df, stores_df, run_dt, scd_rng
    )
    if run_dt.weekday() == 0:
        print("  Monday — dimension changes applied (SCD Type 2 will fire)")
    else:
        print("  No changes — dimensions stable")

    # ── Step 3: Generate fact sales ───────────────────────
    print("\n[3/4] Generating fact sales transactions...")
    random.seed(None)  # ensure facts are non-deterministic
    fact_df = generate_fact_sales(
        products_df, stores_df, customers_df,
        run_date, run_timestamp
    )
    print(f"  Transactions:    {len(fact_df)}")
    print(f"  Total revenue:   ${fact_df['total_amount'].sum():,.2f}")
    print(f"  Avg order value: ${fact_df['total_amount'].mean():,.2f}")
    print(f"  Avg discount:    "
          f"{fact_df['discount_pct'].mean() * 100:.1f}%")

    # ── Step 4: Save CSVs ─────────────────────────────────
    print("\n[4/4] Saving CSV files...")
    date_str = run_date.replace("-", "")
    files = {
        "products":   save_csv(products_df,
                               f"products_{date_str}.csv"),
        "stores":     save_csv(stores_df,
                               f"stores_{date_str}.csv"),
        "customers":  save_csv(customers_df,
                               f"customers_{date_str}.csv"),
        "fact_sales": save_csv(fact_df,
                               f"fact_sales_{date_str}.csv")
    }

    print(f"\nGeneration complete — {run_date}")
    return files


if __name__ == "__main__":
    main()
