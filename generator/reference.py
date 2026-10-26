"""Reference data for the synthetic Malaysian fintech app. All names are fictional combinations."""

from __future__ import annotations

# --- Names (combined at random; no real people) -------------------------------------------
ETHNIC_GROUPS = ("malay", "chinese", "indian")
ETHNIC_WEIGHTS = (65, 26, 9)

MALAY_MALE = (
    "Ahmad", "Muhammad", "Amir", "Hafiz", "Faizal", "Syafiq", "Haziq", "Irfan", "Danial", "Aiman",
    "Firdaus", "Rizal", "Azlan", "Zulkifli", "Hakim", "Imran", "Khairul", "Nazri", "Shahrul", "Ikmal",
)  # fmt: skip
MALAY_FEMALE = (
    "Nur", "Siti", "Aisyah", "Farah", "Nurul", "Aina", "Syahirah", "Liyana", "Hidayah", "Amira",
    "Balqis", "Izzati", "Nadia", "Sofea", "Zulaikha", "Hani", "Atiqah", "Najwa", "Fatimah", "Mardhiah",
)  # fmt: skip
MALAY_FATHER = (
    "Abdullah", "Ismail", "Ibrahim", "Hassan", "Hussein", "Osman", "Yusof", "Rahman", "Razak", "Hamid",
    "Salleh", "Kassim", "Mansor", "Jamal", "Aziz", "Rashid", "Latif", "Bakar", "Omar", "Zakaria",
)  # fmt: skip
CHINESE_SURNAMES = (
    "Tan", "Lim", "Lee", "Ng", "Wong", "Chan", "Ong", "Teh", "Goh", "Chong",
    "Yap", "Leong", "Chin", "Khoo", "Lau", "Low", "Cheah", "Foo", "Liew", "Tay",
)  # fmt: skip
CHINESE_GIVEN = (
    "Wei Jie", "Mei Ling", "Kok Wai", "Hui Min", "Jia Hao", "Xin Yi", "Chee Keong", "Siew Lan", "Zhi Hao",
    "Pei Shan", "Kah Mun", "Boon Huat", "Yee Ling", "Wei Ming", "Shu Fen", "Jun Kit", "Li Ying", "Kai Xiang",
)  # fmt: skip
INDIAN_MALE = ("Arjun", "Rajesh", "Suresh", "Kumar", "Ganesh", "Vijay", "Prakash", "Ravi", "Karthik", "Siva")
INDIAN_FEMALE = ("Priya", "Kavitha", "Lakshmi", "Deepa", "Anitha", "Shalini", "Divya", "Revathi", "Malini", "Nithya")
INDIAN_FATHER = (
    "Muthu", "Rajan", "Subramaniam", "Krishnan", "Raman", "Selvam", "Nair", "Pillai", "Govindasamy", "Maniam",
)  # fmt: skip

# RFC 2606 reserved domains, so no generated address can belong to a real person.
EMAIL_DOMAINS = ("example.com", "example.net", "example.org")
MOBILE_PREFIXES = ("10", "11", "12", "13", "14", "16", "17", "18", "19")

# --- Customers --------------------------------------------------------------------------------
STATES = (
    "Selangor", "Johor", "Sabah", "Perak", "Sarawak", "Kedah", "Kuala Lumpur", "Kelantan",
    "Pulau Pinang", "Pahang", "Negeri Sembilan", "Terengganu", "Melaka", "Perlis", "Putrajaya", "Labuan",
)  # fmt: skip
STATE_WEIGHTS = (22, 12, 10, 8, 8, 6, 6, 6, 5, 5, 4, 4, 3, 1, 0.5, 0.5)

# Monthly income bands (MYR), ordered low to high
INCOME_BANDS = ("below_2k", "2k_4k", "4k_6k", "6k_10k", "above_10k")
INCOME_WEIGHTS = (15, 35, 25, 17, 8)
SALARY_BY_BAND = {"below_2k": 1_700, "2k_4k": 3_000, "4k_6k": 5_000, "6k_10k": 7_800, "above_10k": 13_000}

# --- Accounts ---------------------------------------------------------------------------------
ACCOUNT_TYPES = ("spending", "savings")
ACCOUNT_STATUSES = ("active", "dormant", "closed")

# --- Transactions -----------------------------------------------------------------------------
SPEND_CATEGORIES = (
    "groceries", "food_and_beverage", "transport", "utilities", "telco",
    "shopping", "entertainment", "health", "education", "transfer",
)  # fmt: skip
SPEND_WEIGHTS = (18, 26, 16, 4, 4, 14, 5, 4, 2, 7)
CATEGORIES = (*SPEND_CATEGORIES, "salary")
UNKNOWN_CATEGORIES = ("unknown", "crypto_exchange", "MISC")

# Lognormal amount parameters per category: (median MYR, sigma)
AMOUNT_PARAMS = {
    "groceries": (55, 0.6),
    "food_and_beverage": (18, 0.5),
    "transport": (20, 0.7),
    "utilities": (110, 0.4),
    "telco": (60, 0.3),
    "shopping": (85, 0.8),
    "entertainment": (35, 0.6),
    "health": (65, 0.7),
    "education": (180, 0.6),
    "transfer": (200, 0.9),
}

MERCHANTS = {
    "groceries": ("Jaya Grocer", "Mydin", "Lotus's", "99 Speedmart", "AEON", "Village Grocer", "NSK Trade City"),
    "food_and_beverage": (
        "OldTown White Coffee",
        "Secret Recipe",
        "Marrybrown",
        "ZUS Coffee",
        "Tealive",
        "Mamak Corner",
        "Kopitiam Corner",
    ),
    "transport": ("Grab", "Touch 'n Go", "Petronas", "Shell", "Rapid KL", "KTMB", "AirAsia"),
    "utilities": ("TNB", "Air Selangor", "Indah Water", "Ranhill SAJ"),
    "telco": ("Maxis", "CelcomDigi", "U Mobile", "Unifi", "Yes"),
    "shopping": ("Shopee", "Lazada", "Mr DIY", "Padini", "Zalora", "Watsons", "Guardian"),
    "entertainment": ("GSC Cinemas", "TGV Cinemas", "Netflix", "Spotify", "Astro"),
    "health": ("Caring Pharmacy", "Big Pharmacy", "KPJ Healthcare", "Klinik Mediviron"),
    "education": ("PTPTN", "Popular Bookstore", "MPH Bookstores", "Kumon"),
    "transfer": ("DuitNow Transfer", "IBG Transfer"),
}

CHANNELS = ("card", "duitnow_qr", "fpx", "ewallet", "transfer")
BILL_CATEGORIES = ("utilities", "telco", "education")

TXN_STATUSES = ("completed", "pending", "failed", "reversed")
INITIAL_STATUS_WEIGHTS = (90, 7, 3, 0)

# Relative transaction volume by weekday (Monday=0)
DOW_FACTOR = (0.95, 0.95, 0.97, 1.0, 1.08, 1.12, 0.95)

# Hour-of-day weights (local time) for activity timestamps
HOUR_WEIGHTS = (1, 0.5, 0.3, 0.2, 0.2, 0.5, 2, 4, 5, 5, 5, 6, 8, 7, 5, 5, 5, 6, 7, 8, 8, 6, 4, 2)

# --- Cash advances ----------------------------------------------------------------------------
ADVANCE_STATUSES = ("active", "repaid", "overdue")
ADVANCE_AMOUNTS = (50, 100, 150, 200, 300, 500)
ADVANCE_WEIGHTS = (25, 30, 15, 15, 10, 5)

# --- App events -------------------------------------------------------------------------------
EVENT_TYPES = ("login", "view_offer", "click_offer", "apply_offer")
EVENT_WEIGHTS = (55, 25, 13, 7)
OFFERS = ("OFF-ADV-050", "OFF-ADV-100", "OFF-SAVE-BOOST", "OFF-CASHBACK-GROCER", "OFF-BNPL-INTRO")
DEVICE_OS = ("android", "ios", "harmonyos")
DEVICE_OS_WEIGHTS = (62, 33, 5)
REFERRAL_SOURCES = ("organic", "friend_referral", "social_media", "search_ads", "partner")
REFERRAL_WEIGHTS = (40, 20, 20, 15, 5)
