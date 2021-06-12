import re
import os
import json
import unicodedata
from multiprocessing import Pool
from typing import Dict, Any, Generator

import kss
from absl import app, flags
from tqdm import tqdm

FLAGS = flags.FLAGS

flags.DEFINE_string("dump", "", help='나무위키 덤프 파일 위치')
flags.DEFINE_string("output", "", help='output path')
flags.DEFINE_integer("chars_per_file", 100000000, help='chars per file')

def main(argv):
    assert FLAGS.dump and FLAGS.output

    os.makedirs(FLAGS.output, exist_ok=True)

    with open(FLAGS.dump) as f:
        obj = json.load(f)

    file_index = 0
    file_handler = open(os.path.join(FLAGS.output, f"namu_{file_index:06d}"), 'w')
    num_chars = 0
    with Pool(12) as pool:
        for result in tqdm(pool.imap_unordered(preprocess, obj)):
            for doc in result:
                num_chars += len(doc)
                file_handler.write(f"{doc}\n")

                if num_chars > FLAGS.chars_per_file:
                    file_handler.close()

                    file_index += 1
                    file_handler = open(os.path.join(FLAGS.output, f"namu_{file_index:06d}"), 'w')
                    num_chars = 0


def preprocess(x: Dict[str, Any]) -> Generator[str, None, None]:
    results = []

    titles = [x['title']]

    doc_title = x['title']
    doc_text = f""
    is_in_table = False
    is_in_syntax = False
    is_in_folding = False

    for line in x['text'].split("\n"):
        line: str = line.strip()
        line = unicodedata.normalize("NFC", line)

        if line.startswith("##"):  # 주석
            continue

        if is_in_folding:
            if "}}}" not in line:
                continue
            is_in_folding = False
            line = line[line.index("}}}") + 3:].strip()

        if is_in_syntax:
            if "}}}" not in line:
                continue
            is_in_syntax = False
            line = line[line.index("}}}") + 3:].strip()

        if is_in_table:
            if line.endswith("||"):
                is_in_table = False
            continue

        # [목차], [clearfix], [include ....], [[분류:....]] 등의 텍스트는 스킵
        # 혹시 멀티모달같은거 시도하시려면 여기서 이미지 처리 해주시면 됩니당
        if line.startswith("[") and line.endswith(']'):
            continue

        # 접기 - 중간에 들어갈 경우 어색해짐
        if line.startswith("{{{#!folding"):
            if not line.endswith("}}}"):
                is_in_folding = True
            continue

        # 코드 블럭
        if line.startswith("{{{"): # 원래는 {{{#!syntax 로 시작해야 하지만, {{{만으로 시작하는 것이 많이 보임
            if not line.endswith("}}}"):
                is_in_syntax = True
            continue

        # 표
        if line.startswith("||"):
            if not line.endswith("||"):
                is_in_table = True
            continue

        # redirect, ..
        if line.startswith("#redirect") or line.startswith("#넘겨주기"):
            continue

        # block quotes
        if line.startswith(">"):
            continue

        line = re.sub(r"\[\[[^\]\|]+\|([^\]\|]+)\]\]", r"\1", line)  # 링크
        line = re.sub(r"\[\[([^\]\|]+)#[^\]]+\]\]", r"\1", line)  # anchor가 있는 링크
        line = re.sub(r"\[\[([^\]\|]+)\]\]", r"\1", line)  # 링크
        line = re.sub(r"\[\*[^\]]+\]", r"", line)  # 각주
        line = re.sub(r"\[anchor[^\]]+\]", r"", line)  # anchor

        line = re.sub(r"{{{(#|\+)[a-f0-9]{3,6} ([^}]+)}}}", r"\1", line)  # 색상, 크기

        # 텍스트 스타일
        line = re.sub(r"'''([^']+)'''", r"\1", line) # 굵게
        line = re.sub(r"''([^']+)''", r"\1", line) # 기울임
        line = re.sub(r"___([^_]+)___", r"\1", line) # 기울임
        line = re.sub(r"\(--[^-]+--\)", r"", line)  # 괄호안 취소선을 넣는 사람이 보인다..
        line = re.sub(r"\(~~[^~]+~~\)", r"", line)  # 괄호안 취소선을 넣는 사람이 보인다..
        line = re.sub(r"~~[^~]+~~", r"", line)  # 보통 취소선은 없는게 자연스럽다고 느껴져 삭제
        line = re.sub(r"--[^-]+--", r"", line)  # 취소선
        line = re.sub(r"\^\^[^\^]+\^\^", r"", line)  # 윗첨자 삭제
        line = re.sub(r",,[^,]+,,", r"", line)  # 아랫첨자 삭제
        line = re.sub(r"{{{([^}]+)}}}", r"\1", line)  # 리터럴

        # 문자 이스케이프
        line = re.sub(r"\\([\\_-])", r"\1", line)

        # 섹션 제목
        if line.startswith("==") and line.endswith("=="):
            # title
            if doc_text:
                results.append(f"{doc_title}\n{doc_text}")

            heading = re.sub("^=+ (.+) =+$", "\\1", line).strip()
            heading_level = re.search("^=+", line).span()[1] - 1
            titles = titles[:heading_level]
            titles.append(heading)
            doc_title = ' - '.join(titles)
            doc_text = ""
            continue

        # 휴리스틱
        line = re.sub(r"다. ?#$", r"다.", line)  # 맨 끝에 링크 있는 경우
        line = re.sub(r"다.# ", r"다. ", line)  # 문장 끝에 링크 있는 경우
        line = line.replace("(...)", " ")  # 의미없는 문자열 삭제. Ellipsis 문자 같은 경우에 실제 의미가 있지만, 의미없이 해당문자가 들어가 있는 경우가 많아 삭제
        line = re.sub(r"[\s]+", ' ', line)  # 띄어쓰기 normalize

        # 리스트
        if line.startswith("* "):
            line = line[2:].strip()

        if line == '':
            continue

        sentences = '\n'.join(kss.split_sentences(line, safe=True))
        doc_text += f"{sentences}\n"

    if doc_text:
        results.append(f"{doc_title}\n{doc_text}")

    return results

if __name__ == "__main__":
    app.run(main)
