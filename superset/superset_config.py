FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

ENABLE_PROXY_FIX = True
SECRET_KEY = "YOUR_OWN_RANDOM_GENERATED_STRING"
SUPERSET_WEBSERVER_TIMEOUT = 300
SESSION_COOKIE_SAMESITE = None
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = False
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

EXTRA_CATEGORICAL_COLOR_SCHEMES = [
    {
        "id": 'olympicColors',
        "description": '',
        "label": 'Colors of the Olympic Rings',
        "isDefault": False,
        "colors":
         ['#4594CC', '#FAD749', '#353535', '#43964A', '#BB3D37']
    },
    {
        "id": 'xylophoneColors',
        "description": '',
        "label": 'Colors of a typical toy Xylophone',
	        "isDefault": True,
        "colors":
         ['#FF0000', '#FFA500', '#FFFF00', '#008000', '#0000FF', '#000080', '#663399', '#FFC0CB']
    }
]

MAPBOX_API_KEY = 'pk.eyJ1Ijoienp4eHp6MjU2OSIsImEiOiJjbG9nb2p0ajEweWNkMmpvMTF3aDA1d21zIn0.TDMJTZiR8Op1SohwxJyKIA'