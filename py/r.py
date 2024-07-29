import base64
import ctypes
import hashlib
import json
import logging
import pickle
import copy
import random
import re
import os
import sys
import traceback
from collections.abc import Iterable
from pathlib import Path

# project root directory
RDIR = Path(__file__).resolve().parent.parent
# py directory
PDIR = Path(__file__).resolve().parent

class gv:
    root = 'P:/cs/py/'
    user_Agent = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
    }
    header0 = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-CA,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,en-GB;q=0.6,en-US;q=0.5',
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'Sec-CH-UA': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
        'Sec-CH-UA-Mobile': '?0',
        'Sec-CH-UA-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        # 'Sec-Fetch-User': '?1'
    }
    repl = "⋖⋗≪≫⩽⩾〈〉：？·ᚋ∕⧶／＼⧵"


class ClassToDict(json.JSONEncoder):
    def default(self, o):
        return vars(o)
        # if dataclasses.is_dataclass(o):
        #     return dataclasses.asdict(o)
        # return super().default(o)


def make_cookie(cookie_str: str, domain='') -> dict:
    cookies_dict = {
        "name": 'ck',
        'value': 'ck_a',
        'domain': domain,
    }
    lines = cookie_str.split(';')
    for line in lines:
        key, value = line.strip().split('=', 1)
        cookies_dict[key] = value
    return cookies_dict


def powerset(s):
    x = len(s)
    masks = [1 << i for i in range(x)]
    for i in range(1 << x):
        yield [ss for mask, ss in zip(masks, s) if i & mask]


def str_trans(text: str, src: str, dst: str) -> str:
    return text.translate(text.maketrans(src, dst))


def path_filter(path: str | Path, os='win', op='replace', replace="〈〉：'.？·⧵_") -> Path:
    bad_strs = [
        'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7',
        'COM8', 'COM9', 'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
    ]
    bad_chars = r'<>:"|?*\/' + bytearray([i for i in range(0x20)]).decode('ascii')
    p = str(path)
    if os == 'win':
        if Path(p).stem in bad_strs:
            p = '_' + p
            if op == 'throw':
                raise NameError
        while p[-1] == '.':
            p = p[:-1]
            if op == 'throw':
                raise NameError
        if op == 'throw':
            for i in bad_chars:
                if i in p:
                    raise NameError
        return Path(str_trans(p, bad_chars, replace + ''.join(['0' for i in range(0x20)])))


def to_path(p: Path | str, filter_=False, **kwargs):
    if filter_:
        p = path_filter(p)
    return Path(p)


def rt(path: str | Path, mode='none', encoding='utf-8', **kwargs) -> str | dict | list:
    path = to_path(path, **kwargs)
    return load(path.read_text(encoding), mode, **kwargs)


def rb(path: str | Path, mode='none', **kwargs) -> str | dict | list | bytes:
    path = to_path(path, **kwargs)
    return load(path.read_bytes(), mode, **kwargs)


def wt(path: str | Path, content, mode='none', encoding='utf-8', mkpdir=False, **kwargs) -> Path:
    path = to_path(path, **kwargs)
    if mkpdir:
        mkdir(path.parent)
    path.write_text(dump(content, mode, b64_as_str=True, **kwargs), encoding, **kwargs)
    return path


def wb(path: str | Path, content, mode='none', mkpdir=False, **kwargs) -> Path:
    path = to_path(path, **kwargs)
    if mkpdir:
        mkdir(path.parent)
    path.write_bytes(dump(content, mode, **kwargs))
    return path


def mkdir(path: Path | str, filter_=False) -> Path:
    path = to_path(path, filter_=filter_)
    path.mkdir(parents=True, exist_ok=True)
    return path


def dump(c, mode,
         ensure_ascii=False, encoding='utf-8', b64_as_str=True):
    try:
        if mode == 'none':
            return c
        elif mode == 'json':
            return json.dumps(c, ensure_ascii=ensure_ascii, indent=2, cls=ClassToDict)
        elif mode == 'pk':
            return pickle.dumps(c)
        # elif mode == 'yaml':
        #     return c
        elif mode == 'b64':
            if type(c) == str:
                c = c.encode(encoding)
            return base64.b64encode(c).decode("latin1") if b64_as_str else base64.b64encode(c)

    except Exception as e:
        print(traceback.format_exc())


def load(c, mode,
         encoding='utf-8', b64_as_str=True, **kwargs):
    try:
        if mode == 'none':
            return c
        elif mode == 'json':
            return json.loads(c, **kwargs)
        elif mode == 'pk':
            return pickle.loads(c)
        # elif mode == 'yaml':
        #     return c
        elif mode == 'b64':
            if type(c) == str:
                c = c.encode("latin1")
            return base64.b64decode(c).decode(encoding) if b64_as_str else base64.b64decode(c)
    except Exception as e:
        print(traceback.format_exc())


def sha265(c, inp='s', out='s'):
    if inp == 'pk':
        c = pickle.dumps(c)
    elif inp == 'b':
        pass
    elif inp == 's':
        c = c.encode('utf-8')
    elif inp == '2s':
        c = str(c).encode('utf-8')
    return hashlib.sha256(c).hexdigest() if out == 's' else hashlib.sha256(c)


def de_cp(li, dc=True, lp=False):
    if lp and type(li) == list:
        return [de_cp(i) for i in li]
    if dc:
        return copy.deepcopy(li)


def type_check(content, mode='iter', out=[], depth=-1, index=0):
    if mode == 'iter':
        return isinstance(content, Iterable)


def validate_url(url: str, origin=''):
    url_val = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    if type(url) == str:
        if url[:2] == '//':
            url = 'https:' + url
        elif url[:1] == '/':
            url = origin + url
        if re.match(url_val, url):
            return url


def set_logger(
        log_path='',
        stream=sys.stdout,
        mode='a',
        encoding='utf-8',
        fmt_str='%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s'
):
    handles = []
    if log_path:
        file_handle = logging.FileHandler(log_path, encoding=encoding, mode=mode)
        file_handle.setLevel(logging.DEBUG)
        handles.append(file_handle)
    if stream:
        stream_handle = logging.StreamHandler(stream)
        stream_handle.setLevel(logging.INFO)
        handles.append(stream_handle)

    logging.basicConfig(
        handlers=handles,
        encoding=encoding,
        format=fmt_str,
        level=logging.DEBUG,
        datefmt='%Y-%m-%d/%H:%M:%S',
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def guess_img(img_byte: bytes) -> str:
    # JPEG data in JFIF or Exif format
    if img_byte[6:10] in (b'JFIF', b'Exif') or img_byte.startswith(b'\xff\xd8\xff'):
        return 'jpeg'

    elif img_byte.startswith(b'\211PNG\r\n\032\n'):
        return 'png'

    # GIF ('87 and '89 variants)
    elif img_byte[:6] in (b'GIF87a', b'GIF89a'):
        return 'gif'

    # TIFF (can be in Motorola or Intel byte order)
    elif img_byte[:2] in (b'MM', b'II'):
        return 'tiff'

    # SGI image library
    elif img_byte.startswith(b'\001\332'):
        return 'rgb'

    # PBM (portable bitmap)
    elif len(img_byte) >= 3 and \
            img_byte[0] == ord(b'P') and img_byte[1] in b'14' and img_byte[2] in b' \t\n\r':
        return 'pbm'

    # PGM (portable graymap)
    if len(img_byte) >= 3 and \
            img_byte[0] == ord(b'P') and img_byte[1] in b'25' and img_byte[2] in b' \t\n\r':
        return 'pgm'

    # PPM (portable pixmap)
    if len(img_byte) >= 3 and \
            img_byte[0] == ord(b'P') and img_byte[1] in b'36' and img_byte[2] in b' \t\n\r':
        return 'ppm'

    # Sun raster file
    if img_byte.startswith(b'\x59\xA6\x6A\x95'):
        return 'rast'

    # X bitmap (X10 or X11)
    if img_byte.startswith(b'#define '):
        return 'xbm'

    if img_byte.startswith(b'BM'):
        return 'bmp'

    if img_byte.startswith(b'RIFF') and img_byte[8:12] == b'WEBP':
        return 'webp'

    if img_byte.startswith(b'\x76\x2f\x31\x01'):
        return 'exr'


bad_xml_unichr = [
    (0x00, 0x08), (0x0B, 0x0C), (0x0E, 0x1F), (0x7F, 0x84), (0x86, 0x9F), (0xFDD0, 0xFDDF), (0xFFFE, 0xFFFF)
]
if sys.maxunicode >= 0x10000:
    bad_xml_unichr.extend([
        (0x1FFFE, 0x1FFFF), (0x2FFFE, 0x2FFFF), (0x3FFFE, 0x3FFFF), (0x4FFFE, 0x4FFFF),
        (0x5FFFE, 0x5FFFF), (0x6FFFE, 0x6FFFF), (0x7FFFE, 0x7FFFF), (0x8FFFE, 0x8FFFF),
        (0x9FFFE, 0x9FFFF), (0xAFFFE, 0xAFFFF), (0xBFFFE, 0xBFFFF), (0xCFFFE, 0xCFFFF),
        (0xDFFFE, 0xDFFFF), (0xEFFFE, 0xEFFFF), (0xFFFFE, 0xFFFFF), (0x10FFFE, 0x10FFFF)
    ])

bad_xml_ranges = ["%s-%s" % (chr(low), chr(high)) for (low, high) in bad_xml_unichr]
bad_xml_ranges_re = re.compile(u'[%s]' % u''.join(bad_xml_ranges))


def xml_repl(repl: str, input_str: str):
    return bad_xml_ranges_re.sub(repl, input_str)


def elevated(runas=True):
    try:
        is_elevated = ctypes.windll.shell32.IsUserAnAdmin()
    except:
        is_elevated = False
    if runas and not is_elevated:
        print(sys.executable)
        print(" ".join(sys.argv))
        ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
        sys.exit()
    return is_elevated


def gen_rnd(length=15, allow="1"):
    a = ""
    if "1" in allow:
        a += "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM"
    if "!" in allow:
        a += "!@#$%&*_-+="
    if "~" in allow:
        a += "~`^|\\:;,./?"
    if "(" in allow:
        a += "()<>{}[]'\""

    return [random.randrange(len(a)) for i in range(length)]


if __name__ == '__main__':

    ct = 0
    for i in Path("..").iterdir():
        if i.is_dir() and i.name in ["crates", "src", "py"]:
            for f in i.rglob("*"):
                if f.is_file() and '.idea' not in f.parts and f.suffix not in [".pyc", ".log", ".mediawiki"]:
                    # print(f)
                    ct += f.read_text("utf-8").count("\n")

    print(ct )

    pass
