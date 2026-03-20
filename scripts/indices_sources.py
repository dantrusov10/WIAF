INDEX_SOURCES = [
    {
        "id": "scfi",
        "name": "SCFI",
        "source": "Shanghai Shipping Exchange",
        "url": "https://en.sse.net.cn/indices/scfinew.jsp",
        "kind": "html",
        "patterns": [
            r"SCFI[^\d]{0,80}(\d{3,6}(?:[\.,]\d+)?)",
            r"Shanghai Containerized Freight Index[^\d]{0,100}(\d{3,6}(?:[\.,]\d+)?)",
        ],
        "unit": "index_points",
        "enabled": True,
    },
    {
        "id": "ccfi",
        "name": "CCFI",
        "source": "Shanghai Shipping Exchange",
        "url": "https://en.sse.net.cn/indices/ccfinew.jsp",
        "kind": "html",
        "patterns": [
            r"CCFI[^\d]{0,80}(\d{3,6}(?:[\.,]\d+)?)",
            r"China Containerized Freight Index[^\d]{0,100}(\d{3,6}(?:[\.,]\d+)?)",
        ],
        "unit": "index_points",
        "enabled": True,
    },
    {
        "id": "wci",
        "name": "WCI",
        "source": "Drewry",
        "url": "https://www.drewry.co.uk/supply-chain-advisors/supply-chain-expertise/world-container-index-assessed-by-drewry",
        "kind": "html",
        "patterns": [
            r"World Container Index[^\d]{0,120}(\d{3,6}(?:[\.,]\d+)?)",
            r"WCI[^\d]{0,80}(\d{3,6}(?:[\.,]\d+)?)",
        ],
        "unit": "usd_per_40ft",
        "enabled": True,
    },
    {
        "id": "fbx",
        "name": "FBX",
        "source": "Freightos",
        "url": "https://fbx.freightos.com/",
        "kind": "html",
        "patterns": [
            r"Freightos Baltic Index[^\d]{0,120}(\d{3,6}(?:[\.,]\d+)?)",
            r"FBX[^\d]{0,80}(\d{3,6}(?:[\.,]\d+)?)",
        ],
        "unit": "index_points",
        "enabled": True,
    },
]
