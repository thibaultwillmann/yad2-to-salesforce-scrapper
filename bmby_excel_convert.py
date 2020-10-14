# Imports
import pandas as pd
import numpy as np
import re  # regex
import simple_salesforce  # salesforce REST API client
import requests  # teams notification
import json


# File Path
file_name = '' # TODO


# for failure detection
execution_state = "No mapping yet"


# Mapping Definitions
cities = {
    "גבעתיים": ["Givatayim", 4],
    "הרצליה": ["Hertzeliya", 2],
    "כפר סבא": ["Kfar Saba", 1],
    "נתניה": ["Netanya", 3],
    "פתח תקווה": ["Petah Tikva", 0],
    "רמת גן": ["Ramat Gan", 5],
    "ראשון לציון": ["Rishon Letzion", -1],
    "תל אביב יפו": ["Tel Aviv Yafo", 6],
    "בת ים": ["Bat Yam", -1],
    "חולון": ["Holon", -1]
}


neighborhoods = [
    # Petah Tikva
    {
        "אחדות": "אחדות",
        "אם המושבות הותיקה": "אם המושבות",
        "הדר המושבות הותיקה": "אם המושבות",
        "אם המושבות החדשה": "הדר המושבות",
        "הדר המושבות החדשה": "הדר המושבות",
        "האחים ישראלית": "האחים ישראלית",
        "מרכז העיר": "מרכז העיר",
        "מרכז העיר צפון": "מרכז העיר צפון",
        "נחלת צבי": "נחלת צבי",
        "ביה\"ח השרון": "מרכז העיר דרום",
        "מרכז העיר דרום": "מרכז העיר דרום",
        "התשעים ושלוש": "מרכז העיר דרום",
        "שבדיה": "מרכז העיר דרום",
        "בלינסון": "בלינסון",
        "כפר אברהם": "כפר אברהם",
        "כפר גנים": "כפר גנים",
        "כפר גנים א'": "כפר גנים א",
        "כפר גנים ב'": "כפר גנים ב",
        "כפר גנים ג'": "כפר גנים ג",
        "רמת ורבר": "רמת ורבר",
        "האחים בן עזר": "רמת ורבר",
        "בר יהודה": "בר יהודה",
        "קרית חזני": "קרית חזני",
        "בת גנים": "בת גנים",
        "גני הדר": "גני הדר",
        "מחנה יהודה": "מחנה יהודה",
        "קרית ארגוב": "מחנה יהודה",
        "עמישב": "עמישב",
        "הדר גנים": "הדר גנים",
        "נוה גן": "נווה גן",
        "נווה גנים": "נווה גנים",
        "עין גנים": "עין גנים",
        "קרית המשוררים": "עין גנים",
        "צמרת גנים": "עין גנים דרום",
        "נווה מעוז": "נווה מעוז (נווה דקלים)",
        "נווה עוז": "נווה עוז",
        "לוי אשכול": "נווה עוז",
        "נווה עוז החדשה": "נווה עוז הירוקה",
        "נווה עוז מערב": "נווה עוז הירוקה",
        "מניה שוחט": "נווה עוז הירוקה",
        "יוספטל": "יוספטל",
        "שעריה": "שערייה",
        "סגולה": "סגולה",
        "קרית אריה": "קרית אריה",
        "קרול": "קרול",
        "קריית הרב סלומון": "קרית הרב סולמון",
        "קרית אלון": "קרית אלון",
        "קרית אליעזר פרי": "קרית אליעזר פרי",
        "קרית ספיר": "קרית אליעזר פרי",
        "קרית דוד אלעזר": "קרית דוד אליעזר",
        "שיפר": "שיפר",
        "קריית מטלון": "קרית מטלון",
        "קרית משה": "קרית משה (קרית מטלון)",
        "רמת סיב": "רמת סיב (פסגת הדר)",
        "משהב": "משהב",
        "שיכון מפ\"מ": "שיכון מפ\"ם",
        "הפועל המזרחי": "שיכון פועלי המזרחי",
        "שיכון הפועל המזרחי ותיקים": "שיכון פועלי המזרחי",
        "חן הצפון": "שיכון פועלי המזרחי",
        "צומת סירקין": "צומת סירקין",
        "כפר סירקין": "כפר סירקין (מושב)",
        "כפר מעש": "כפר מעש (מושב)",
        "גת רימון": "גת רימון (מושב)",
        "משכנות גנים": "משכנות גנים (מושב)",
        "פגה": "פגה",
        "אזור תעשיה סגולה": "פארק עסקים ירקון",
        "אזור תעשיה קרית אריה": "פארקטק פתח תקווה",
        "אזור תעשיה רמת סיב, קרית מטלון": "סטרטסיב",
        "קרית מטלון": "קרית מטלון",
        "אזור תעשייה": "אחר",
        "בר אילן": "כפר אברהם",
        "האיזור השקט": "אחר",
        "קריית אריה": "פארקטק פתח תקווה",
        "תקומה, קרית חזני": "קרית חזני",
        "נאות בגין": "אחר",
        "בני בתך": "אחר"
    },
    # Kfar Saba
    {
        "אלי כהן": "אלי כהן",
        "אליעזר": "אליעזר",
        "בית ונוף": "בית ונוף",
        "חלוצים": "גאולים",
        "גאולים": "גאולים",
        "גבעת אשכול": "גבעת אשכול ( קדמת הדרים)",
        "תשלו\"ז": "גני השרון - תשלו\"ז",
        "גני השרון": "גני השרון - תשלו\"ז",
        "הדרים": "שכונת הדרים הוותיקה",
        "הדרים,הדרים החדשהה": "שכונת הדרים החדשה",
        "הזמר העברי": "כפר סבא הירוקה-דרום (הירוקה 60)",
        "כפר סבא הירוקה-דרום": "כפר סבא הירוקה-דרום (הירוקה 60)",
        "הירוקה": "הירוקה",
        "שכון העובד": "שכונת הפרחים / דגניה",
        "פרחים": "שכונת הפרחים / דגניה",
        "דגניה": "שכונת הפרחים / דגניה",
        "כפר סבא הצעירה": "כפר סבא הצעירה/ האוניברסיטה",
        "האוניברסיטה,כפר סבא הצעירה": "כפר סבא הצעירה/ האוניברסיטה",
        "הראשונים": "הראשונים",
        "ותיקים": "שיכון וותיקים",
        "שיכון ותיקים": "שיכון וותיקים",
        "יוספטל": "יוספטל",
        "כיסופים": "כיסופים",
        "כסופים": "כיסופים",
        "מרכז העיר": "מרכז העיר",
        "משהב": "משה דיין צפון - משה\"ב",
        "סביוני הכפר": "סביוני הכפר",
        "סירקין": "סירקין",
        "שיכון עליה": "שיכון עלייה",
        "עליה": "שיכון עלייה",
        "קפלן": "קפלן",
        "קפלן,בנה ביתך": "קפלן",
        "הפארק": "שכונת הפארק",
        "אזור הפארק": "שכונת הפארק",
        "תקומה": "תקומה",
        "פועלים א'": "פועלים א",
        "פועלים ב'": "פועלים ב",
        "פועלים ד'": "פועלים ד",
        "אזור תעשייה עתיר ידע": "אזור התעשיה עתיר ידע",
        "למפרט": "למפרט",
        "שבזי": "שבזי",
        "מעוז": "מעוז",
        "קרית ספיר": "קרית ספיר",
        "העובד": "עובד בן ציון",
        "עובד בן ציון": "עובד בן ציון",
        "א.ת המזרחי": "א.ת המזרחי",
        "אזור המלאכה": "אזור המלאכה",
        "מזרחי א'": "מזרחי א'",
        "אחר": "אחר",
        "גני אלרם": "גני אלרם",
        "גרין": "גרין",
        "מוצקין": "דובדבן הכפר - המרכז השקט",
        "דובדבן הכפר": "דובדבן הכפר - המרכז השקט",
        "הרב שמחה אסף": "דובדבן הכפר - המרכז השקט",
        "חצרות הדר": "חצרות הדר ( סמוך לקניון ערים)",
        "חצרות הדר ב'": "חצרות הדר ב' ( סמוך לקניון ערים)",
        "כפר סבא הירוקה -צפון": "כפר סבא הירוקה -צפון (הירוקה 80)",
        "משכנות": "משכנות ( סמוך לאז\"ר)",
        "מתחם אז\"ר": "מתחם אז\"ר",
        "נוה הדרים": "נוה הדרים",
        "עלומים": "עלומים",
        "פרוגרסיבים": "פרוגרסיבים",
        "רום העיר": "רום העיר",
        "רופין": "רופין"
    },
    # Herzliya
    {
        "גורדון": "גורדון",
        "גן הרש\"ל": "גן רש\"ל",
        "גן רש\"ל": "גן רש\"ל",
        "גני הרצליה": "גני הרצליה",
        "הרצליה ב'": "הרצליה ב",
        "גבעת הסופר": "הרצליה ב",
        "דרום": "דרום",
        "שיכון דרום": "דרום",
        "שיכון החיילים המשוחררים": "הרצליה הירוקה",
        "הירוקה": "הרצליה הירוקה",
        "הרצליה הירוקה": "הרצליה הירוקה",
        "נווה עובד": "הרצליה הירוקה",
        "גבעת הפרחים": "הרצליה הצעירה",
        "הרצליה הצעירה": "הרצליה הצעירה",
        "שיכון עולים": "הרצליה הצעירה",
        "חוף התכלת": "הרצליה פיתוח",
        "הרצליה פיתוח": "הרצליה פיתוח",
        "וייצמן": "וייצמן",
        "ויצמן": "וייצמן",
        "נוה אמירים": "נווה אמירים",
        "נווה ישראל": "נווה ישראל",
        "נווה עמל": "נווה עמל",
        "נוף ים": "נוף ים",
        "נחלת עדה": "נחלת עדה",
        "מרכז העיר": "מרכז העיר",
        "יד התשעה": "יד התשעה",
        "שביב": "יד התשעה",
        "מרינה": "מרינה",
        "גליל ים (מתחם הכוכבים)": "גליל ים",
        "אזור תעשיה": "אזור התעשיה",
        "מערב העיר": "אחר",
        "שאר העיר": "אחר",
        "אביבי הרצליה": "אחר"
    },
    # Netanya
    {
        "האגמים": "אגמים",
        "אגמים": "אגמים",
        "אזור תעשיה קרית אליעזר, אזור תעשיה הישן": "אזור התעשייה קריית אליעזר (אזור תעשיה ישן, קריית אליעזר)",
        "קריית אליעזר - אזור תעשיה": "אזור התעשייה קריית אליעזר (אזור תעשיה ישן, קריית אליעזר)",
        "אזור תעשיה ספיר, אזור תעשיה פולג": "אזור תעשייה - ספיר",
        "ביג פולג": "אזור תעשייה - ספיר",
        "קריית ספיר - אזור תעשיה": "אזור תעשייה - ספיר",
        "בן ציון": "בן ציון",
        "גבעת האירוסים": "גבעת האירוסים",
        "גלי ים": "גלי הים",
        "מחנה יעקב": "מחנה יעקב",
        "מכנס משה": "מכנס (מערב העיר)",
        "מרכז/כיכר העצמאות": "מכנס (מערב העיר)",
        "אזור הים": "מכנס (מערב העיר)",
        "מרכז העיר": "מרכז העיר",
        "משכנות זבולון": "משכנות זבולון",
        "נאות גולדה": "נאות גולדה",
        "נאות גנים": "נאות גנים",
        "נאות גנים ותיקים": "נאות גנים",
        "נאות הרצל": "נאות הרצל",
        "סלע": "נאות הרצל",
        "עמידר": "נאות הרצל",
        "גן ברכה": "נאות הרצל",
        "נאות מנחם בגין": "נאות מנחם בגין",
        "נאות שקד": "נאות שקד",
        "נוה איתמר": "נוה איתמר",
        "נווה שלום": "נווה שלום",
        "נאות התכלת": "נווה שלום",
        "גלי ים (נת/600 דרום)": "נת/600, נוף הטיילת",
        "נוף הטיילת": "נת/600, נוף הטיילת",
        "גלי ים, נוף הטיילת, נווה עוז": "נת/600, נוף הטיילת",
        "עין התכלת": "עין התכלת",
        "עיר ימים": "עיר ימים",
        "פרדס הגדוד": "פרדס הגדוד",
        "קריית יהלום": "קריית יהלום",
        "קרית נורדאו": "קרית נורדאו",
        "קרית צאנז": "קרית צאנז",
        "רמת אפרים": "רמת אפרים",
        "רמת חן ובן ציון": "רמת חן",
        "דורה": "רמת יגאל ידין - דורה",
        "רמת ידין, דורה": "רמת יגאל ידין - דורה",
        "רמת פולג": "רמת פולג",
        "קרית השרון": "קריית השרון",
        "קרית רבין": "קריית יצחק רבין",
        "נווה עוז": "נווה עוז",
        "מרכז העיר דרום": "דרום מרכז העיר",
        "צפון העיר": "צפון מרכז העיר",
        "צפון מזרח מרכז העיר": "צפון מרכז העיר",
        "צפון מערב מרכז העיר": "צפון מרכז העיר",
        "עיר": "אחר",
        "נתניה": "אחר",
        "אחר": "אחר",
        "שאר העיר": "אחר",
        "שיכון המזרחי": "אחר",
        "אזור תעשייה קרית יהלום בתיכנון": "אחר",
        "טוברוק": "טוברוק",
    },
    # Givatayim
    {
        "אחר": "אחר",
        "ארלוזורוב": "ארלוזורוב",
        "בורוכוב": "בורוכוב",
        "בן צבי": "בן צבי",
        "גבעת הרמבם": "גבעת רמב\"ם",
        "גבעתיים": "אחר",
        "חברת חשמל": "חברת חשמל",
        "סיטי גבעתיים": "סיטי",
        "סירקין": "סירקין",
        "פועלי הרכבת": "פועלי הרכבת",
        "קרית יוסף": "קרית יוסף",
        "שטח 9": "שטח 9",
        "שיכון עממי גימל": "שיכון עממי גימל",
        "שינקין": "שינקין",
        "תל גנים": "תל גנים",
    },
    # Ramat Gan
    {
        "אחר": "מתחם נגבה",
        "אפעל": "רמת אפעל",
        "בורסה": "הבורסה",
        "בר אילן": "בר אילן",
        "גבול ר\"ג בני ברק": "גבול בני ברק",
        "גבעת גאולה": "גבעת גאולה",
        "גבעת הברושים": "גבעת הברושים",
        "הבילויים": "אזור הבילוים",
        "הברושים": "הברושים",
        "הפועל המזרחי ב'": "הפועל המזרחי ב'",
        "חרוזים": "חרוזים",
        "חרות": "חרות",
        "יד לבנים": "יד לבנים",
        "כפר אז\"ר": "כפר אז\"ר",
        "מזרחי": "שיכון מזרחי",
        "מרום נווה": "מרום נווה",
        "מרכז העיר": "מרכז העיר א",
        "מרכז העיר ב'": "מרכז העיר ג",
        "מרכז העיר ג'": "מרכז העיר ב",
        "נגבה": "מתחם נגבה",
        "נוה יהושע, ערמונים": "נוה יהושע",
        "נוה ערמונים": "נוה ערמונים",
        "נווה אפעל": "נוה אפעל",
        "נחלת גנים": "נחלת גנים",
        "עלית": "עלית",
        "ערמונים": "נוה ערמונים",
        "פארק לאומי": "פארק לאומי",
        "קרית בורוכוב": "בורוכוב",
        "קרית בורכוב": "בורוכוב",
        "קרית קריניצי": "קרית קריניצי",
        "קרית קריניצי / תל השומר": "קרית קריניצי",
        "רמת אפעל": "רמת אפעל",
        "רמת דניה": "רמת דניה",
        "רמת השקמה": "רמת השקמה",
        "רמת חן": "רמת חן",
        "רמת יצחק": "רמת יצחק",
        "רמת עמידר": "רמת עמידר",
        "שיכון הבנים": "שיכון הבנים",
        "שיכון מזרחי": "שיכון מזרחי",
        "שיכון עלית": "שיכון עלית",
        "שיכון צבא הקבע": "שיכון צבא הקבע",
        "שיכון צנחנים": "שיכון צנחנים",
        "שכונות הלל": "הלל",
        "שכונת בן גוריון, מרכז העיר ג'": "מרכז העיר ג",
        "שכונת גפן": "הגפן",
        "שכונת הותיקים": "שכונת ותיקים",
        "שכונת הלל": "הלל",
        "שכונת הראשונים": "שכונת הראשונים",
        "שכונת חשמונאים, מרכז העיר א": "מרכז העיר א",
        "שנקר": "חרוזים",
        "תל בנימין": "תל בנימין",
        "תל גנים": "תל גנים",
        "תל השומר": "תל השומר",
        "תל יהודה": "תל יהודה",
        "תל ליטווינסקי": "תל ליטווינסקי", 
    },
    # Tel Aviv Yafo
    {
        "אוניברסיטת תל אביב": "רמת אביב",
        "אורות": "אורות",
        "אזור בזל": "הצפון הישן - החלק הצפוני",
        "אזור תעסוקה צומת חולון": "אזור תעסוקה צומת חולון",
        "אזורי חן, גימל החדשה, צוקי אביב": "אזורי חן",
        "אחר": "אחר",
        "איזור חוף הים": "הצפון הישן - החלק הדרומי",
        "איזור שדה דב": "תכנית ל",
        "אפקה": "אפקה",
        "בבלי": "בבלי",
        "ביצרון ורמת ישראל": "ביצרון ורמת ישראל",
        "בית דגן": "בית דגן",
        "בית יעקב": "בית יעקב",
        "גבעת הפרחים": "גבעת הפרחים",
        "גבעת הרצל, בלומפילד": "בלומפילד",
        "גבעת עמל ב'": "גבעת עמל",
        "גימל החדשה": "רמת אביב החדשה",
        "גלילות, סי אנד סאן": "גלילות",
        "גני צהלה, רמות צהלה": "גני צהלה",
        "גני שרונה, קרית הממשלה": "גני שרונה",
        "דיזינגוף סנטר": "לב העיר",
        "דרום": "דרום",
        "דרך לוד": "דרך לוד",
        "הבימה": "לב העיר",
        "הגוש הגדול": "הגוש הגדול",
        "הדר יוסף": "הדר יוסף",
        "הכובשים": "כרם התימנים",
        "המושבה האמריקאית-גרמנית": "המושבה האמריקאית-גרמנית",
        "המשתלה": "המשתלה",
        "הצפון החדש - דרום": "הצפון החדש - החלק הדרומי",
        "הצפון החדש - כיכר המדינה": "הצפון החדש - סביבת כיכר המדינה",
        "הצפון החדש - צפון": "הצפון החדש - החלק הצפוני",
        "הצפון הישן - דרום": "הצפון הישן - החלק הדרומי",
        "הצפון הישן - צפון": "הצפון הישן - החלק הצפוני",
        "הקריה": "לב העיר",
        "הרכבת": "הרכבת",
        "התקווה": "התקווה",
        "יד אליהו": "יד אליהו",
        "יהודה המכבי": "הצפון הישן - החלק הצפוני",
        "יפו ד', גבעת התמרים": "יפו ד', גבעת התמרים",
        "יפו העתיקה": "יפו העתיקה",
        "ישגב": "ישגב",
        "כוכב הצפון": "כוכב הצפון",
        "כפיר, נווה כפיר": "כפיר, נוה כפיר",
        "כרם התימנים": "כרם התימנים",
        "לב תל אביב, לב העיר צפון": "לב העיר",
        "לבנה": "לב העיר",
        "מגדל שלום": "לב העיר",
        "מגדלי נאמן": "מגדלי נאמן",
        "מונטיפיורי": "מונטיפיורי",
        "מכללת תל אביב יפו, דקר": "מכללת תל אביב יפו, דקר",
        "מעוז אביב": "מעוז אביב",
        "מרינה": "הצפון הישן - החלק הצפוני",
        "מרכז הירידים": "מרכז הירידים",
        "מרכז העיר": "לב העיר",
        "נאות אפקה א'": "נאות אפקה א",
        "נאות אפקה ב'": "נאות אפקה ב",
        "נוה אביבים": "נווה אביבים וסביבתה",
        "נוה אליעזר, כפר שלם מזרח": "נוה אליעזר, כפר שלם מזרח",
        "נוה ברבור, כפר שלם מערב": "נוה ברבור, כפר שלם מערב",
        "נוה גולן, יפו ג'": "נוה גולן, יפו ג'",
        "נוה גן": "נוה גן",
        "נוה דן": "נוה דן",
        "נוה חן": "נוה חן",
        "נוה צדק": "נוה צדק",
        "נוה צה''ל": "נוה צה''ל",
        "נוה שאנן": "נוה שאנן",
        "נווה עופר": "נוה עופר",
        "נווה שרת": "נוה שרת",
        "נופי ים": "נופי ים",
        "נחלת בנימין": "נחלת בנימין",
        "נחלת יצחק": "נחלת יצחק",
        "ניר אביב": "ניר אביב",
        "נמל תל אביב": "הצפון הישן - החלק הצפוני",
        "עג'מי, גבעת העליה": "עג'מי, גבעת העליה",
        "עזרא, הארגזים": "עזרא, הארגזים",
        "עתידים": "עתידים",
        "פארק דרום": "פארק דרום",
        "פארק החורשות": "פארק החורשות",
        "פארק הירקון": "הצפון הישן - החלק הצפוני",
        "פארק צמרת": "פארק צמרת",
        "פלורנטין": "פלורנטין",
        "צהלה": "צהלה",
        "צהלון, שיכוני חסכון": "צהלון, שיכוני חסכון",
        "צוקי אביב": "צוקי אביב",
        "צמרות איילון": "פארק צמרת",
        "צמרת איילון, פארק צמרת": "פארק צמרת",
        "צפון": "הצפון הישן - החלק הצפוני",
        "צפון יפו, המושבה האמריקאית-גרמנית": "המושבה האמריקאית-גרמנית",
        "קו הים": "קו הים",
        "קרית המלאכה": "קריית המלאכה",
        "קרית שאול": "קריית שאול",
        "קרית שלום": "קריית שלום",
        "רביבים": "רביבים",
        "רמות צהלה": "צהלה",
        "רמת אביב": "רמת אביב",
        "רמת אביב ג'": "רמת אביב ג'",
        "רמת אביב החדשה": "רמת אביב החדשה",
        "רמת החייל": "רמת החייל",
        "רמת הטייסים": "רמת הטייסים",
        "רמת ישראל": "ביצרון ורמת ישראל",
        "שארית ישראל": "שארית ישראל",
        "שבזי": "שבזי",
        "שד' ההשכלה": "שד' ההשכלה",
        "שייח' מוניס": "שייח' מוניס",
        "שיכון דן, נוה דן": "נוה דן",
        "שיכון הקצינים": "שיכון הקצינים",
        "שיכון חיסכון": "שיכון חיסכון",
        "שיכון עממי ג'": "שיכון עממי ג'",
        "שמעון": "שמעון",
        "שפירא": "שפירא",
        "שקם": "שקם",
        "תכנית ל', למד": "תכנית ל",
        "תל ברוך": "תל ברוך",
        "תל ברוך צפון": "תל ברוך",
        "תל חיים": "תל חיים",
        "תל- כביר נוה עופר,יפו ב'": "תל- כביר נוה עופר,יפו ב",
    }
]


def getCityName(city):
    try:
        return cities[city][0]
    except:
        return ""


def getCityID(city):
    try:
        return cities[city][1]
    except:
        return -1


def getNeighborhoodName(city, neighborhood):
    index = getCityID(city)
    if index == -1:
        return ""
    try:
        return neighborhoods[index][neighborhood]
    except:
        if neighborhood != '':
            print('New BMBY neighborhood detected: ' + str(neighborhood))
        return ""


def validate_number(current_value):
    try:
        new_value_str = re.search('(\d+)', current_value).group(1)
        return new_value_str
    except AttributeError:
        return None


balcony_mapping = {
    -1.0: '1',
    np.nan: '0'
}


apartment_type_mapping = {
    "דירה": "Apartment",
    "דירת קרקע": "Ground Apartment",
    "דירת גן": "Garden Apartment",
    "דירת גג": "Penthouse/Rooftop Apartment",
    "מיני פנטהאוז": "Mini Penthouse",
    "פנטהאוז": "Penthouse/Rooftop Apartment",
    "'קוטג": "Cottage",
    "קוטג' דו-משפחתי": "Two Family Cottage",
    "וילה": "Villa",
    "בית": "House",
    "דופלקס": "Duplex",
    "טריפלקס": "Triplex",
    "לופט": "Studio / loft",
    "דירת סטודיו": "Studio / loft",
    "יחידת דיור": "Residential Unit",
    "דירה מחולקת": "Splited Apartment",
    "דירה לחלוקה": "Apartment for Split",
    "דירת נופש": "Holiday unit",
    "אחר": "Other"
}


elevator_mapping = {
    'לא': '0',
    'כן': '1'
}


storage_mapping = {
    -1.0: '1',
    np.nan: '0'
}


parking_mapping = {
    '4+': '5',
    4: '4',
    3: '3',
    2: '2',
    1: '1',
    0: '0',
    '': '0',
    np.nan: '0'
}


air_condition_mapping = {
    'לא': False,
    'כן': True,
    '': False,
    np.nan: False
}


# format first name
def drop_DB(current_value):
    try:
        new_value = re.search('DB \((.*)\)', current_value).group(1)
    except AttributeError:
        # no match found in the original string
        new_value = current_value
    return new_value


# format family name
def split_first_name(df):
    for row_index in df.First_Name__c.index:
        if df.at[row_index, 'Last_Name__c'] == '':
            split_name = df.at[row_index, 'First_Name__c'].rsplit(' ', maxsplit=1)
            if len(split_name) == 2:
                df.at[row_index, 'Last_Name__c'] = split_name[1]
                df.at[row_index, 'First_Name__c'] = split_name[0]
            elif len(split_name) == 1:
                df.at[row_index, 'Last_Name__c'] = '‎'
                df.at[row_index, 'First_Name__c'] = split_name[0]


def parse_date_time(bmby_format):
    if not bmby_format:
        return ""
    date = bmby_format[:10]
    time = bmby_format[11:]
    return date + 'T' + time + '.000+0300'


# import excel file into Dataframe and format all fields according to Salesforce
def prepareData():

    # read file into a DataFrame
    df = pd.read_excel(file_name)
    print('BMBY Import for file ' + str(file_name) + ' has started.')
    global execution_state

    # first_name column -> extract the name from DB () -> First_Name__c column
    df['first_name'] = df['first_name'].apply(str)
    df.rename(columns={'first_name': 'First_Name__c'}, inplace=True)
    df['First_Name__c'] = df['First_Name__c'].map(lambda name_withDB: drop_DB(name_withDB))
    df['First_Name__c'] = df['First_Name__c'].replace('nan', '', regex=True)
    execution_state = 'first_name -> First_Name__c mapping completed'

    # family_name column -> Last_Name__c column
    df.rename(columns={'family_name': 'Last_Name__c'}, inplace=True)
    df['Last_Name__c'] = df['Last_Name__c'].astype(str)
    df['Last_Name__c'] = df['Last_Name__c'].replace('nan', '', regex=True)
    split_first_name(df)
    execution_state = 'family_name -> Last_Name__c mapping completed'

    # last_communication_date column -> format time/date -> Last_communication_on_BMBY__c column
    df.rename(columns={'last_communication_date': 'Last_communication_on_BMBY__c'}, inplace=True)
    df['Last_communication_on_BMBY__c'] = df['Last_communication_on_BMBY__c'].astype(str)
    df['Last_communication_on_BMBY__c'] = df['Last_communication_on_BMBY__c'].replace('NaT', '', regex=True)
    execution_state = 'last_communication_date -> Last_communication_on_BMBY__c mapping completed'

    # registration_date column -> format time/date -> Created_Date__c column
    df.rename(columns={'registration_date': 'Created_Date__c'}, inplace=True)
    df['Created_Date__c'] = df['Created_Date__c'].astype(str)
    df['Created_Date__c'] = df['Created_Date__c'].replace('NaT', '', regex=True)
    df['Created_Date__c'] = df['Created_Date__c'].map(lambda bmby_date: parse_date_time(bmby_date))
    execution_state = 'registration_date -> Created_Date__c mapping completed'

    # address column -> pba__Address_pb__c column
    df.rename(columns={'address': 'pba__Address_pb__c'}, inplace=True)
    df['pba__Address_pb__c'] = df['pba__Address_pb__c'].astype(str)
    df['pba__Address_pb__c'] = df['pba__Address_pb__c'].replace('nan', '', regex=True)
    execution_state = 'address -> pba__Address_pb__c mapping completed'

    # address (seller) -> Seller_address__c text
    df.rename(columns={'address (seller)': 'Seller_address__c'}, inplace=True)
    df['Seller_address__c'] = df['Seller_address__c'].astype(str)
    df['Seller_address__c'] = df['Seller_address__c'].replace('nan', '', regex=True)
    execution_state = 'address (seller) -> Seller_address__c mapping completed'

    # city (seller) -> match Salesforce city names -> Seller_city__c text
    df.rename(columns={'city (seller)': 'Seller_city__c'}, inplace=True)
    df['Seller_city__c'] = df['Seller_city__c'].map(lambda bmby_city_name: getCityName(bmby_city_name))
    execution_state = 'city (seller) -> Seller_city__c mapping completed'

    # Nighborhood column -> match Salesforce neighborhood names -> Neighborhoods_HH__c column
    df.rename(columns={'Nighborhood': 'Neighborhoods_HH__c'}, inplace=True)
    for row_index in df.Neighborhoods_HH__c.index:
        df.at[row_index, 'Neighborhoods_HH__c'] = getNeighborhoodName(df.at[row_index, 'Ctiy'], df.at[row_index, 'Neighborhoods_HH__c'])
    execution_state = 'Nighborhood -> Neighborhoods_HH__c mapping completed'

    # Ctiy column -> match Salesforce city names -> Property_City__c column
    df.rename(columns={'Ctiy': 'Property_City__c'}, inplace=True)
    df['Property_City__c'] = df['Property_City__c'].map(lambda bmby_city_name: getCityName(bmby_city_name))
    execution_state = 'Ctiy -> Property_City__c mapping completed'

    # Phone_number column -> get rid of any text -> Mobile__c column
    df.rename(columns={'Phone_number': 'Mobile__c'}, inplace=True)
    df['Mobile__c'] = df['Mobile__c'].astype('str')
    df['Mobile__c'] = df['Mobile__c'].map(lambda phone_number: validate_number(phone_number))
    df.Mobile__c.fillna(value='', inplace=True)
    execution_state = 'Phone_number -> Mobile__c mapping completed'

    # cellphone_nubmer column -> Second_mobile__c column
    df.rename(columns={'cellphone_nubmer': 'Second_mobile__c'}, inplace=True)
    df['Second_mobile__c'] = df['Second_mobile__c'].astype('str')
    df['Second_mobile__c'] = df['Second_mobile__c'].map(lambda phone_number: validate_number(phone_number))
    df.Second_mobile__c.fillna(value='', inplace=True)
    execution_state = 'cellphone_nubmer -> Second_mobile__c mapping completed'

    # Comment column -> BMBY_Comment__c column
    df.rename(columns={'Comment': 'BMBY_Comment__c'}, inplace=True)
    execution_state = 'Comment -> BMBY_Comment__c mapping completed'

    # additional_phone_number column -> add depending on existing cellphone_nubmer -> Comment__c column or Second_mobile__c column
    df['additional_phone_number'] = df['additional_phone_number'].astype('str')
    df['additional_phone_number'] = df['additional_phone_number'].map(lambda phone_number: validate_number(phone_number))
    df.additional_phone_number.fillna(value='', inplace=True)
    for row_index in df.additional_phone_number.index:
        if not df.at[row_index, 'additional_phone_number'] == '':
            if df.at[row_index, 'Second_mobile__c'] == '':
                df.at[row_index, 'Second_mobile__c'] = df.at[row_index, 'additional_phone_number']
            else:
                df.at[row_index, 'BMBY_Comment__c'] = df.at[row_index, 'BMBY_Comment__c'] + '\n' + 'Additional Phone Number: ' + df.at[row_index, 'additional_phone_number']
    df = df.drop('additional_phone_number', 1)

    # email column -> Email__c column
    df.rename(columns={'email': 'Email__c'}, inplace=True)
    df['Email__c'] = df['Email__c'].astype('str')
    df['Email__c'] = df['Email__c'].replace('nan', '', regex=True)
    execution_state = 'email -> Email__c mapping completed'

    # rooms column -> round up -> Room_HH__c column
    df.rename(columns={'rooms': 'Room_HH__c'}, inplace=True)
    df['Room_HH__c'] = df['Room_HH__c'].round()
    df['Room_HH__c'] = df['Room_HH__c'].astype("Int64")
    df['Room_HH__c'] = df['Room_HH__c'].astype(str)
    df['Room_HH__c'] = df['Room_HH__c'].replace('<NA>', '', regex=True)
    execution_state = 'rooms -> Room_HH__c mapping completed'

    # floor column -> pba__Floor__c column
    df.rename(columns={'floor': 'pba__Floor__c'}, inplace=True)
    df['pba__Floor__c'] = df['pba__Floor__c'].astype("Int64")
    df['pba__Floor__c'] = df['pba__Floor__c'].astype(str)
    df['pba__Floor__c'] = df['pba__Floor__c'].replace('<NA>', '', regex=True)
    execution_state = 'floor -> pba__Floor__c mapping completed'

    # balcony column -> 0 / 1 -> Balcony__c column
    df.rename(columns={'balcony': 'Balcony__c'}, inplace=True)
    df['Balcony__c'] = df['Balcony__c'].map(balcony_mapping)
    execution_state = 'balcony -> Balcony__c mapping completed'

    # action_type column -> only keep "sell"
    df.drop(df[df['action_type'] != 'מכירה'].index, inplace=True)
    df = df.drop('action_type', 1)
    execution_state = 'action_type dropping completed'

    # agent column -> Agent__c column
    df.rename(columns={'agent': 'Agent__c'}, inplace=True)
    df['Agent__c'] = df['Agent__c'].astype(str)
    df['Agent__c'] = df['Agent__c'].replace('nan', '', regex=True)
    execution_state = 'agent -> Agent__c mapping completed'

    # source column -> all bmby -> Source__c column
    df.rename(columns={'source': 'Source__c'}, inplace=True)
    df['Source__c'] = 'bmby'
    execution_state = 'source -> Source__c mapping completed'

    # price column -> pba__ListingPrice_pb__c column
    df.rename(columns={'price': 'pba__ListingPrice_pb__c'}, inplace=True)
    df['pba__ListingPrice_pb__c'] = df['pba__ListingPrice_pb__c'].replace('[\$,]', '', regex=True)
    df['pba__ListingPrice_pb__c'] = df['pba__ListingPrice_pb__c'].astype(float)
    execution_state = 'price -> pba__ListingPrice_pb__c mapping completed'

    # sqaure_meters column -> pba__Area_pb__c column
    df.rename(columns={'sqaure_meters': 'pba__Area_pb__c'}, inplace=True)
    df['pba__Area_pb__c'] = df['pba__Area_pb__c'].astype(str)
    df['pba__Area_pb__c'] = df['pba__Area_pb__c'].replace('nan', '', regex=True)
    execution_state = 'sqaure_meters -> pba__Area_pb__c mapping completed'

    # apartment_type column -> drop all listings with other types -> pba__PropertyType__c column
    df.rename(columns={'apartment_type': 'pba__PropertyType__c'}, inplace=True)
    df = df.drop(df[df['pba__PropertyType__c'] == 'קרוואן'].index)
    df = df.drop(df[df['pba__PropertyType__c'] == 'משק/חווה'].index)
    df = df.drop(df[df['pba__PropertyType__c'] == 'בניין מגורים'].index)
    df = df.drop(df[df['pba__PropertyType__c'] == 'קבוצת רכישה'].index)
    df['pba__PropertyType__c'] = df['pba__PropertyType__c'].map(lambda bmby_type: apartment_type_mapping.get(bmby_type, 'אחר'))
    df['pba__PropertyType__c'].replace('', np.nan, inplace=True)
    df.dropna(subset=['pba__PropertyType__c'], inplace=True)
    execution_state = 'apartment_type -> pba__PropertyType__c mapping completed'

    # elevator column -> 0 / 1 -> Elavator__c column
    df.rename(columns={'elevator': 'Elavator__c'}, inplace=True)
    df['Elavator__c'] = df['Elavator__c'].map(elevator_mapping)
    df['Elavator__c'] = df['Elavator__c'].astype(str)
    df['Elavator__c'] = df['Elavator__c'].replace('nan', '', regex=True)
    execution_state = 'elevator -> Elavator__c mapping completed'

    # parking column -> 0 / 1 / 2 / 3 / 4 / 5 -> Number_of_Parking_space__c column
    df.rename(columns={'parking': 'Number_of_Parking_space__c'}, inplace=True)
    df['Number_of_Parking_space__c'] = df['Number_of_Parking_space__c'].map(parking_mapping)
    df['Number_of_Parking_space__c'] = df['Number_of_Parking_space__c'].astype(str)
    df['Number_of_Parking_space__c'] = df['Number_of_Parking_space__c'].replace('nan', '', regex=True)
    execution_state = 'parking -> Number_of_Parking_space__c mapping completed'

    # storage column -> 0 / 1 -> Storage_Area__c column
    df.rename(columns={'storage': 'Storage_Area__c'}, inplace=True)
    df['Storage_Area__c'] = df['Storage_Area__c'].map(storage_mapping)
    df['Storage_Area__c'] = df['Storage_Area__c'].astype(str)
    df['Storage_Area__c'] = df['Storage_Area__c'].replace('nan', '', regex=True)
    execution_state = 'storage -> Storage_Area__c mapping completed'

    # building_condition column -> Building_condition__c column
    df.rename(columns={'building_condition': 'Building_condition__c'}, inplace=True)
    df['Building_condition__c'] = df['Building_condition__c'].astype(str)
    df['Building_condition__c'] = df['Building_condition__c'].replace('nan', '', regex=True)
    execution_state = 'building_condition -> Building_condition__c mapping completed'

    # building_age column -> Building_age__c column
    df.rename(columns={'building_age': 'Building_age__c'}, inplace=True)
    df['Building_age__c'] = df['Building_age__c'].astype(str)
    df['Building_age__c'] = df['Building_age__c'].replace('nan', '', regex=True)
    execution_state = 'building_age -> Building_age__c mapping completed'

    # air_conditioner column -> Air_conditioner__c column
    df.rename(columns={'air_conditioner': 'Air_conditioner__c'}, inplace=True)
    df['Air_conditioner__c'] = df['Air_conditioner__c'].map(air_condition_mapping)
    df['Air_conditioner__c'] = df['Air_conditioner__c'].astype(bool)
    execution_state = 'air_conditioner -> Air_conditioner__c mapping completed'

    # bmby_id column -> bmby_ID__c column
    df.rename(columns={'bmby_id': 'bmby_ID__c'}, inplace=True)
    df['bmby_ID__c'] = df['bmby_ID__c'].astype(str)
    df['bmby_ID__c'].replace('nan', '', inplace=True)
    execution_state = 'bmby_id -> bmby_ID__c mapping completed'

    # bmby comment column -> Comment__c column
    df['משימה\פגישה אחרונה'] = df['משימה\פגישה אחרונה'].astype(str)
    df['משימה\פגישה אחרונה'].replace('nan', '', inplace=True)
    df['BMBY_Comment__c'] = df['BMBY_Comment__c'] + '\nמשימה\פגישה אחרונה:' + df['משימה\פגישה אחרונה']
    df = df.drop('משימה\פגישה אחרונה', 1)
    execution_state = 'משימה\פגישה אחרונה column -> BMBY_Comment__c mapping completed'

    #Evacuation date column -> Evacuation_date__c column
    df.rename(columns={'Evacuation date': 'Evacuation_date__c'}, inplace=True)
    df['Evacuation_date__c'] = df['Evacuation_date__c'].astype(str)
    df['Evacuation_date__c'].replace('nan', '', inplace=True)
    execution_state = 'Evacuation date -> Evacuation_date__c mapping completed'

    #print('BMBY -> Salesforce data mapping completed')
    return df


# open session to CRM API user
def openSalesforceConnection():
    session_id, instance = simple_salesforce.SalesforceLogin(username='', # TODO
                                                             password='',
                                                             security_token='')
    sf = simple_salesforce.Salesforce(instance=instance, session_id=session_id)
    return sf


# create listing & contact / update listing in Salesforce row by row through Dataframe
def sendListings(sf, df, event):

    global execution_state
    execution_state = 'No listings imported yet'
    
    for row_index in df.First_Name__c.index:
        
        print(row_index)
        
        try:

            # get existing contact (Mobile)
            newContact = sf.query("SELECT id FROM Contact WHERE MobilePhone = '0" + df.at[row_index, "Mobile__c"].replace("-", "") + "' LIMIT 1")
            contact_id = newContact["records"][0]["Id"]
            print("Contact found & assigned: " + contact_id) ###
            
        except:
                
            # create new contact
            newContact = sf.Contact.create({
                'First_Name_HH__c': (str(df.at[row_index, 'First_Name__c']) if str(df.at[row_index, 'First_Name__c']) != '' else 'unknown'),
                'FirstName': (str(df.at[row_index, 'First_Name__c']) if str(df.at[row_index, 'First_Name__c']) != '' else 'unknown'),
                'LastName': (str(df.at[row_index, 'Last_Name__c']) if str(df.at[row_index, 'Last_Name__c']) != '' else 'unknown'),
                'lastNameForFormula__c': (str(df.at[row_index, 'Last_Name__c']) if str(df.at[row_index, 'Last_Name__c']) != '' else 'unknown'),
                'pba__ContactType_pb__c': 'Seller',
                'MobilePhone': str(df.at[row_index, 'Mobile__c']),
                'Second_mobile__c': str(df.at[row_index, 'Second_mobile__c']),
                'Email': str(df.at[row_index, 'Email__c']),
                'Status_HH__c': 'SellerStatus',
                'OwnerId': '0054J000002EgHFQA0'
            })
            contact_id = newContact["id"] ###
            print("Contact created: " + contact_id) ###

        try:

            existingListing = sf.query("SELECT id FROM pba__Listing__c WHERE bmby_ID__c = '" + str(df.at[row_index, 'bmby_ID__c']) + "' LIMIT 1")
        
            try:
            
                existingListingId = existingListing['records'][0]['Id']
        
            except:

                existingListing = sf.query("SELECT id FROM pba__Listing__c WHERE pba__Address_pb__c = '" + str(df.at[row_index, 'pba__Address_pb__c']) + "' AND Mobile__c = '0" + str(df.at[row_index, 'Mobile__c'].replace("-", "")) + "' AND pba__Floor__c = '" + str(df.at[row_index, 'pba__Floor__c']) + "' LIMIT 1")
                existingListingId = existingListing['records'][0]['Id']
            
            sf.pba__Listing__c.update(str(existingListingId), {
                'First_Name__c': str(df.at[row_index, 'First_Name__c']),
                'Last_Name__c': str(df.at[row_index, 'Last_Name__c']),
                'Last_communication_on_BMBY__c': str(df.at[row_index, 'Last_communication_on_BMBY__c']),
                'Created_Date__c': str(df.at[row_index, 'Created_Date__c']),
                'pba__Address_pb__c': str(df.at[row_index, 'pba__Address_pb__c']),
                'Seller_address__c': str(df.at[row_index, 'Seller_address__c']),
                'Seller_city__c': str(df.at[row_index, 'Seller_city__c']),
                'Property_City__c': str(df.at[row_index, 'Property_City__c']),
                'Neighborhoods_HH__c': str(df.at[row_index, 'Neighborhoods_HH__c']),
                'Mobile__c': str(df.at[row_index, 'Mobile__c']),
                'Second_mobile__c': str(df.at[row_index, 'Second_mobile__c']),
                'Email__c': str(df.at[row_index, 'Email__c']),
                'Room_HH__c': str(df.at[row_index, 'Room_HH__c']),
                'pba__Floor__c': str(df.at[row_index, 'pba__Floor__c']),
                'Balcony__c': str(df.at[row_index, 'Balcony__c']),
                'Source__c': str(df.at[row_index, 'Source__c']),
                'pba__ListingPrice_pb__c': (str(df.at[row_index, 'pba__ListingPrice_pb__c']) if not np.isnan(df.at[row_index, 'pba__ListingPrice_pb__c']) else ''),
                'pba__OriginalListPrice_pb__c': (str(df.at[row_index, 'pba__ListingPrice_pb__c']) if not np.isnan(df.at[row_index, 'pba__ListingPrice_pb__c']) else ''),
                'pba__SqFt_pb__c': str(df.at[row_index, 'pba__Area_pb__c']),
                'pba__PropertyType__c': str(df.at[row_index, 'pba__PropertyType__c']),
                'Elavator__c': str(df.at[row_index, 'Elavator__c']),
                'Number_of_Parking_space__c': str(df.at[row_index, 'Number_of_Parking_space__c']),
                'Storage_Area__c': str(df.at[row_index, 'Storage_Area__c']),
                'Building_condition__c': str(df.at[row_index, 'Building_condition__c']),
                'Building_age__c': str(df.at[row_index, 'Building_age__c']),
                'Air_conditioner__c': ('true' if df.at[row_index, 'Air_conditioner__c'] else 'false'),
                'BMBY_Screenshot_URL__c': (getURL(df.at[row_index, 'bmby_ID__c']) if event.get('i_fill_history_sceenshot', False) else ''),
                'Evacuation_date__c': str(df.at[row_index, 'Evacuation_date__c']),
                'BMBY_Comment__c': str(df.at[row_index, 'BMBY_Comment__c']),
                'Owning_work_on_the_property__c': 'Private',
                'Status_CS__c': 'New',
                'Number_of_missed_calls__c': '0',
                'Seller_side_commission_signed__c': '0',
                'type__c': 'Properties'
            })
            
            execution_state = str(existingListingId)
            print("existing listing updated: " + str(existingListingId))
        
        except:

            newListing = sf.pba__Listing__c.create({
                'First_Name__c': str(df.at[row_index, 'First_Name__c']),
                'Last_Name__c': str(df.at[row_index, 'Last_Name__c']),
                'Last_communication_on_BMBY__c': str(df.at[row_index, 'Last_communication_on_BMBY__c']),
                'Created_Date__c': str(df.at[row_index, 'Created_Date__c']),
                'pba__Address_pb__c': str(df.at[row_index, 'pba__Address_pb__c']),
                'Seller_address__c': str(df.at[row_index, 'Seller_address__c']),
                'Seller_city__c': str(df.at[row_index, 'Seller_city__c']),
                'Property_City__c': str(df.at[row_index, 'Property_City__c']),
                'Neighborhoods_HH__c': str(df.at[row_index, 'Neighborhoods_HH__c']),
                'Mobile__c': str(df.at[row_index, 'Mobile__c']),
                'Second_mobile__c': str(df.at[row_index, 'Second_mobile__c']),
                'Email__c': str(df.at[row_index, 'Email__c']),
                'Room_HH__c': str(df.at[row_index, 'Room_HH__c']),
                'pba__Floor__c': str(df.at[row_index, 'pba__Floor__c']),
                'Balcony__c': str(df.at[row_index, 'Balcony__c']),
                'Source__c': str(df.at[row_index, 'Source__c']),
                'pba__ListingPrice_pb__c': (str(df.at[row_index, 'pba__ListingPrice_pb__c']) if not np.isnan(df.at[row_index, 'pba__ListingPrice_pb__c']) else ''),
                'pba__OriginalListPrice_pb__c': (str(df.at[row_index, 'pba__ListingPrice_pb__c']) if not np.isnan(df.at[row_index, 'pba__ListingPrice_pb__c']) else ''),
                'pba__SqFt_pb__c': str(df.at[row_index, 'pba__Area_pb__c']),
                'pba__PropertyType__c': str(df.at[row_index, 'pba__PropertyType__c']),
                'Elavator__c': str(df.at[row_index, 'Elavator__c']),
                'Number_of_Parking_space__c': str(df.at[row_index, 'Number_of_Parking_space__c']),
                'Storage_Area__c': str(df.at[row_index, 'Storage_Area__c']),
                'Building_condition__c': str(df.at[row_index, 'Building_condition__c']),
                'Building_age__c': str(df.at[row_index, 'Building_age__c']),
                'Air_conditioner__c': ('true' if df.at[row_index, 'Air_conditioner__c'] else 'false'),
                'bmby_ID__c': str(df.at[row_index, 'bmby_ID__c']),
                'OwnerId': '0054J000002EgHFQA0',
                'BMBY_Screenshot_URL__c': (getURL(df.at[row_index, 'bmby_ID__c']) if event.get('i_fill_history_sceenshot', False) else ''),
                'pba__PropertyOwnerContact_pb__c': str(contact_id),
                'Evacuation_date__c': str(df.at[row_index, 'Evacuation_date__c']),
                'BMBY_Comment__c': str(df.at[row_index, 'BMBY_Comment__c']),
                'Owning_work_on_the_property__c': 'Private',
                'Status_CS__c': 'New',
                'Number_of_missed_calls__c': '0',
                'Seller_side_commission_signed__c': '0',
                'type__c': 'Properties'
            })

            newListingInfo = sf.query("SELECT pba__Property__c FROM pba__Listing__c WHERE ID = '" + str(newListing['id']) + "' LIMIT 1")
            
            sf.pba__PropertyMedia__c.create({
                'pba__Category__c': 'Images',
                'pba__Property__c': str(newListingInfo['records'][0]['pba__Property__c']),
                'pba__IsExternalLink__c': 'true',
                'pba__ExternalLink__c': '', # TODO
                'pba__IsOnExpose__c': 'true'
            })

            sf.Contact.update(str(contact_id), {
                'Assigned_Property__c': str(newListing['id'])
            })

            execution_state = str(newListing['id'])
            print("new listing created: " + str(newListing['id']))
    
    return None

# fill URL for Screenshot if i_fill_history_sceenshot = true
def getURL(bmby_id):
    if bmby_id == '':
        return ''
    response = requests.get('' + str(bmby_id) + '.png') # TODO
    if response.status_code == 200:
        return '' + str(bmby_id) + '.png' # TODO
    else:
        return ''


def handleExcel(event, lambda_context):
    global execution_state
    try:
        df = prepareData()
        sf = openSalesforceConnection()
        sendListings(sf, df, event)
    except:
        print("failed at: " + execution_state)
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps({
            "message": "Completed Sucessfully"
        }),
    }

#handleExcel({'i_fill_history_sceenshot': 'true'},0)
