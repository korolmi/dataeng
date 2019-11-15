def hasAttribs(t):
    """ проверяет наличие атрибутов в узле и
    исключает вырожденные атрибуты типа {http://www.w3.org/2001/XMLSchema-instance}nil """

    if not t.attrib:
        return False

    for a in t.attrib:
        if a.find('{http://www.w3.org/2001/XMLSchema-instance}nil')>=0:
            return False

    return True

def etree_to_dict(t):
    """ конвертилка распарсенного XML в словарь """
    
    # удалим наймспейс из тэга
    if t.tag.find('{')>=0:
        t.tag = t.tag.split('}')[1]

    # учтем здесь {http://www.w3.org/2001/XMLSchema-instance}nil
    d = {t.tag: {} if hasAttribs(t) else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if hasAttribs(t):
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d

def getByDot(d,ss):
    """ выбирает из словаря d элемент по строке ss в точечной нотации """

    for e in ss.split("."):
        if not isinstance(d, (dict,)):
            return None
        if e in d.keys():
            d = d[e]
        else:
            return None

    return d
