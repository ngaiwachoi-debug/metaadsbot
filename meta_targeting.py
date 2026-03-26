"""Human-readable targeting text from Meta targeting object (used in Refined sheet)."""

LANG_MAP = {6: "英文", 24: "繁體中文", 64: "菲律賓語(Tagalog)", 31: "簡體中文"}


def parse_targeting_details(targ) -> str:
    if not targ:
        return "預設/廣泛定位"
    details = []
    geo = targ.get("geo_locations", {})
    countries = geo.get("countries", [])
    cities = geo.get("cities", [])
    geo_desc = f"地區: {', '.join(countries)}" if countries else "地區: 未指定國家"
    if cities:
        city_names = [f"{c.get('name')}(+{c.get('radius')}{c.get('distance_unit')})" for c in cities]
        geo_desc += f" | 城市: {', '.join(city_names)}"
    details.append(geo_desc)
    age_min = targ.get("age_min", 18)
    age_max = targ.get("age_max", 65)
    genders = targ.get("genders", [1, 2])
    gender_desc = "女性" if genders == [2] else "男性" if genders == [1] else "不限性別"
    details.append(f"年齡: {age_min}-{age_max} | 性別: {gender_desc}")
    lang_ids = targ.get("languages", [])
    if lang_ids:
        langs = [LANG_MAP.get(lid, f"ID:{lid}") for lid in lang_ids]
        details.append(f"語言: {', '.join(langs)}")
    else:
        details.append("語言: 不限")
    interests = []
    for spec in targ.get("flexible_spec", []):
        for key in ["interests", "behaviors", "life_events"]:
            items = spec.get(key, [])
            interests.extend([i.get("name", "未知項目") for i in items])
    if interests:
        unique_interests = list(dict.fromkeys(interests))
        details.append(f"興趣/行為: {', '.join(unique_interests[:10])}")
    return "\n".join(details)
