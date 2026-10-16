# -*- coding: utf-8 -*-
"""
Script cập nhật báo cáo Word từ báo cáo tiến độ → báo cáo hoàn thiện cuối kỳ.
Dựa theo GhiChu_NoiDung_BoSung_ChinhSua_BaoCao_HoanThien.md
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from docx import Document
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import copy

SRC  = r'D:\DAIHOC\NĂM 4 -HK1\BigData\DoAn_BigData\BaoCao_TienDo_DoAn_BigData_NHOM_2_HoanThien (1).docx'
DEST = r'D:\DAIHOC\NĂM 4 -HK1\BigData\DoAn_BigData\BaoCao_HoanThien_DoAn_BigData_NHOM_2.docx'

doc = Document(SRC)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS – chèn trực tiếp vào XML, không dùng doc.paragraphs.index()
# ─────────────────────────────────────────────────────────────────────────────

def _make_para_xml(doc_obj, text, style_name, bold=False, color=None, center=False):
    """Tạo phần tử <w:p> mới với text và style."""
    p = OxmlElement('w:p')
    # pPr (paragraph properties)
    pPr = OxmlElement('w:pPr')
    pStyle = OxmlElement('w:pStyle')
    # tìm style id
    style_id = style_name.replace(' ', '')
    for s in doc_obj.styles:
        if s.name == style_name:
            style_id = s.style_id
            break
    pStyle.set(qn('w:val'), style_id)
    pPr.append(pStyle)
    if center:
        jc = OxmlElement('w:jc')
        jc.set(qn('w:val'), 'center')
        pPr.append(jc)
    p.append(pPr)
    # run
    r = OxmlElement('w:r')
    rPr = OxmlElement('w:rPr')
    if bold:
        b = OxmlElement('w:b')
        rPr.append(b)
    if color:
        clr = OxmlElement('w:color')
        clr.set(qn('w:val'), color)
        rPr.append(clr)
    if rPr:
        r.append(rPr)
    t = OxmlElement('w:t')
    t.set('{http://www.w3.org/XML/1998/namespace}space', 'preserve')
    t.text = text
    r.append(t)
    p.append(r)
    return p

def insert_after_xml(ref_para, text, style_name='Compact', bold=False, color=None, center=False):
    """Chèn đoạn văn mới sau ref_para (trực tiếp trên XML). Trả về para element mới."""
    new_p = _make_para_xml(doc, text, style_name, bold=bold, color=color, center=center)
    ref_para._element.addnext(new_p)
    # wrap thành Paragraph object
    from docx.text.paragraph import Paragraph
    return Paragraph(new_p, ref_para._element.getparent())

def insert_heading_after_xml(ref_para, text, level=2):
    style_name = f'Heading {level}'
    return insert_after_xml(ref_para, text, style_name=style_name, bold=True)

def add_image_placeholder_after(ref_para, fig_num, fig_caption):
    """
    Chèn sau ref_para:
      1. Đoạn placeholder màu cam
      2. Caption
    Trả về caption paragraph.
    """
    ph_text = f'[CHỜ HÌNH ẢNH – Hình {fig_num}. {fig_caption}]'
    ph_p = insert_after_xml(ref_para, ph_text, style_name='Normal',
                             bold=True, color='CC4400', center=True)
    cap_p = insert_after_xml(ph_p, f'Hình {fig_num}. {fig_caption}',
                              style_name='Caption', center=True)
    return cap_p

def replace_para_text(para, new_text, bold=None):
    """Xóa toàn bộ runs và ghi lại text mới."""
    for run in para.runs:
        run.text = ''
    if para.runs:
        para.runs[0].text = new_text
        if bold is not None:
            para.runs[0].bold = bold
    else:
        run = para.add_run(new_text)
        if bold is not None:
            run.bold = bold

def find_para(keyword, start=0, style=None):
    """Tìm paragraph đầu tiên chứa keyword, trả về (index, paragraph) hoặc (-1, None)."""
    for i, p in enumerate(doc.paragraphs):
        if i < start:
            continue
        if keyword in p.text:
            if style is None or p.style.name == style:
                return i, p
    return -1, None

def add_table_row(table, cells_data):
    row = table.add_row()
    for i, txt in enumerate(cells_data):
        if i < len(row.cells):
            for para in row.cells[i].paragraphs:
                para.clear()
            row.cells[i].paragraphs[0].add_run(txt)

# ─────────────────────────────────────────────────────────────────────────────
# 1. PHẦN BÌA – đổi tiêu đề báo cáo
# ─────────────────────────────────────────────────────────────────────────────
print("1. Cập nhật tiêu đề báo cáo...")
for para in doc.paragraphs:
    if 'BÁO CÁO TIẾN ĐỘ ĐỒ ÁN MÔN HỌC' in para.text:
        replace_para_text(para, 'BÁO CÁO HOÀN THIỆN ĐỒ ÁN MÔN HỌC (BÁO CÁO TỔNG KẾT CUỐI KỲ)', bold=True)
        print("   ✓ Cập nhật tiêu đề bìa")
        break

for para in doc.paragraphs:
    if 'Báo cáo tiến độ' in para.text and ('tháng' in para.text.lower() or 'giai đoạn' in para.text.lower()):
        replace_para_text(para, 'Báo cáo hoàn thiện – Tháng 10/2026')
        print("   ✓ Cập nhật dòng phụ tiêu đề")
        break

# ─────────────────────────────────────────────────────────────────────────────
# 2. BẢNG PHÂN CÔNG NHIỆM VỤ (Table 0)
# ─────────────────────────────────────────────────────────────────────────────
print("2. Cập nhật bảng phân công...")
table0 = doc.tables[0]
members = {
    1: (
        "- Xây dựng cụm HDFS 2 DataNodes và Kafka KRaft broker vận hành không cần ZooKeeper.\n"
        "- Xây dựng kịch bản tự động hóa 1-click khởi động toàn cụm phân tán (START_SYSTEM.bat, start_all.sh).\n"
        "- Mở rộng Kafka Event Producer thu nhận luồng sự kiện thời gian thực đa hành vi "
        "(rate, add_to_watchlist, remove_from_watchlist)."
    ),
    2: (
        "- Xây dựng pipeline tiền xử lý, làm sạch và nén dữ liệu Snappy Parquet (Tầng Silver).\n"
        "- Tối ưu hóa cấu trúc lưu trữ và đọc nạp ma trận đặc trưng tiềm ẩn tầng Gold trên HDFS (movie_factors).\n"
        "- Kiểm thử tính bền vững và khả năng chịu lỗi của cụm 2 DataNode khi lưu trữ phân tán."
    ),
    3: (
        "- Huấn luyện mô hình ALS phân rã ma trận trên PySpark MLlib và trích xuất itemFactors.\n"
        "- Phát triển động cơ gợi ý Hybrid (Franchise-Aware & Contrast Strategy).\n"
        "- Phát triển tính năng Mục Phim Yêu Thích (Watchlist) và thuật toán gợi ý Pseudo-User Latent Profile Vector.\n"
        "- Tích hợp TMDb Poster API chuẩn HD và hoàn thiện giao diện Web Netflix Recommender."
    ),
}
for row_idx, new_text in members.items():
    if row_idx < len(table0.rows):
        cell = table0.rows[row_idx].cells[3]
        for para in cell.paragraphs:
            para.clear()
        cell.paragraphs[0].add_run(new_text)
print("   ✓ Bảng phân công đã cập nhật")

# ─────────────────────────────────────────────────────────────────────────────
# 3. CHƯƠNG I – Mục tiêu và Bảng 2 HDFS
# ─────────────────────────────────────────────────────────────────────────────
print("3. Cập nhật Chương I...")

# Mục tiêu số 3
for para in doc.paragraphs:
    if 'sinh Top 5 phim gợi ý' in para.text:
        replace_para_text(para,
            'Mục tiêu số 3: Huấn luyện mô hình ALS phân rã ma trận trên Spark MLlib, trích xuất ma trận vector '
            'đặc trưng tiềm ẩn (Item Factor Matrix) lưu trữ tại tầng Gold trên HDFS (/lakehouse/gold/movie_factors), '
            'làm nền tảng cho động cơ gợi ý tức thời (On-the-fly Recommender Engine).')
        print("   ✓ Mục tiêu số 3")
        break

# Mục tiêu số 4
for para in doc.paragraphs:
    if 'Thu nhận' in para.text and 'Kafka' in para.text and 'add_to_watchlist' not in para.text:
        if 'Mục tiêu' in para.text or ('log' in para.text.lower() and 'hành vi' in para.text):
            replace_para_text(para,
                'Mục tiêu số 4: Thu nhận luồng tương tác thời gian thực từ người dùng cuối trên Web bằng Apache Kafka KRaft '
                'với các loại sự kiện phong phú (rate, add_to_watchlist, remove_from_watchlist).')
            print("   ✓ Mục tiêu số 4")
            break

# Mục tiêu số 6
for para in doc.paragraphs:
    if 'kiểm tra dữ liệu' in para.text and ('giao diện' in para.text.lower() or 'dashboard' in para.text.lower()):
        replace_para_text(para,
            'Mục tiêu số 6: Xây dựng ứng dụng Web tương tác hoàn chỉnh cho phép người dùng tìm kiếm phim, '
            'chấm sao đánh giá (1★ đến 5★), quản lý danh sách phim yêu thích (Watchlist), tự động kích hoạt '
            'chiến lược gợi ý tương ứng và đồng bộ sự kiện vào cụm Big Data.')
        print("   ✓ Mục tiêu số 6")
        break

# Phát biểu bài toán – Cold-Start
for para in doc.paragraphs:
    if 'Cold-Start' in para.text or ('người dùng mới' in para.text.lower() and 'bài toán' in para.text.lower()):
        if 'Pseudo-User' not in para.text:
            old = para.text
            new = old + (' Hệ thống giải quyết triệt để vấn đề Cold-Start: không bắt buộc người dùng web '
                         'phải mượn danh userId có sẵn; bất kỳ người dùng nào cũng có thể chọn phim bất kỳ '
                         'hoặc lưu nhiều phim vào Watchlist để hệ thống tức thời tính toán danh sách gợi ý tối ưu.')
            replace_para_text(para, new)
            print("   ✓ Phát biểu bài toán – Cold-Start")
            break

# Bảng 2 HDFS – bổ sung movie_factors
table2 = doc.tables[2]
for i, row in enumerate(table2.rows):
    if 'Gold' in row.cells[0].text or ('gold' in row.cells[1].text.lower()):
        path_cell = row.cells[1]
        if 'movie_factors' not in path_cell.text:
            path_cell.paragraphs[-1].add_run('\n/lakehouse/gold/movie_factors/')
        desc_cell = row.cells[-1]
        if 'movie_factors' not in desc_cell.text:
            desc_cell.paragraphs[-1].add_run(
                '\nChứa 19.550 vector đặc trưng ẩn (k=10) từ mô hình ALS, dung lượng ~1.8 MB. '
                'Nền tảng cho động cơ gợi ý On-the-fly.')
        print("   ✓ Bảng 2 – bổ sung movie_factors")
        break

# ─────────────────────────────────────────────────────────────────────────────
# 4. BẢNG 3 – Bổ sung công nghệ TMDb và Kafka KRaft
# ─────────────────────────────────────────────────────────────────────────────
print("4. Cập nhật Bảng 3 (Công nghệ)...")
table3 = doc.tables[3]
if 'TMDb' not in str([r.cells[0].text for r in table3.rows]):
    add_table_row(table3, [
        'The Movie Database (TMDb) API', 'v3', 'Serving Layer (Web API)',
        'Cung cấp poster hình ảnh có bản quyền chuẩn HD cho hơn 42.000 bộ phim hiển thị trên giao diện Web.'
    ])
if 'KRaft' not in str([r.cells[0].text for r in table3.rows]):
    add_table_row(table3, [
        'Apache Kafka KRaft Mode', '3.6.2', 'Streaming Layer',
        'Vận hành không cần ZooKeeper. Thu nhận đa sự kiện người dùng (rate, add_to_watchlist, remove_from_watchlist).'
    ])
print("   ✓ Bảng 3 cập nhật")

# ─────────────────────────────────────────────────────────────────────────────
# 5. GIAI ĐOẠN 6 – Bổ sung mô tả itemFactors
# ─────────────────────────────────────────────────────────────────────────────
print("5. Cập nhật Giai đoạn 6 (itemFactors)...")
_, p_rmse = find_para('RMSE')
if p_rmse and '0.817' in p_rmse.text and 'itemFactors' not in p_rmse.text:
    item_text = (
        'Ngoài việc đánh giá RMSE = 0.817, mô hình ALS trích xuất ma trận nhân tử ẩn V (itemFactors) '
        'gồm 19.550 phim, lưu tại /lakehouse/gold/movie_factors/ trên HDFS. '
        'Mỗi vector có kích thước rank=10, đại diện tọa độ bộ phim trong không gian sở thích người dùng tiềm ẩn. '
        'Đây là nền tảng cốt lõi của động cơ gợi ý On-the-fly ở giai đoạn Serving.'
    )
    insert_after_xml(p_rmse, item_text, style_name='Compact')
    print("   ✓ Bổ sung mô tả itemFactors")

# ─────────────────────────────────────────────────────────────────────────────
# 6. GIAI ĐOẠN 7 – Cosine Similarity và giải quyết Clipping
# ─────────────────────────────────────────────────────────────────────────────
print("6. Cập nhật Giai đoạn 7 (Cosine Similarity)...")
_, p_g7 = find_para('Giai đoạn 7')
if p_g7:
    # Tìm đoạn cuối giai đoạn 7 (trước Giai đoạn 8)
    end_p = p_g7
    for i, para in enumerate(doc.paragraphs):
        if para is p_g7:
            for j in range(i+1, len(doc.paragraphs)):
                next_p = doc.paragraphs[j]
                if 'Heading' in next_p.style.name and 'Giai đoạn 8' in next_p.text:
                    break
                end_p = next_p
            break
    cosine_text = (
        'Phương pháp tính độ tương đồng Cosine giữa hai vector phim:\n'
        '    Cosine Similarity(vi, vj) = (vi · vj) / (‖vi‖ × ‖vj‖)\n'
        'Điểm cosine luôn chuẩn hóa trong đoạn [0, 1] (0% – 100%), '
        'loại bỏ hoàn toàn lỗi điểm số vượt ngưỡng 5.0 từ báo cáo tiến độ (7.10, 7.89...).'
    )
    insert_after_xml(end_p, cosine_text, style_name='Compact')
    print("   ✓ Bổ sung Cosine Similarity")

# ─────────────────────────────────────────────────────────────────────────────
# 7. GIAI ĐOẠN 8 – Kafka đa hành vi + Placeholder hình
# ─────────────────────────────────────────────────────────────────────────────
print("7. Cập nhật Giai đoạn 8 (Kafka)...")
_, p_g8 = find_para('Giai đoạn 8')
if p_g8:
    end_p8 = p_g8
    for i, para in enumerate(doc.paragraphs):
        if para is p_g8:
            for j in range(i+1, len(doc.paragraphs)):
                next_p = doc.paragraphs[j]
                if 'Heading' in next_p.style.name and next_p is not p_g8:
                    break
                end_p8 = next_p
            break

    kafka_text = (
        'Hàm send_kafka_event() được tích hợp trong FastAPI (main.py), hỗ trợ 3 loại sự kiện:\n'
        '  • rate – Đánh giá sao (1★ – 5★)\n'
        '  • add_to_watchlist – Thêm phim vào danh sách yêu thích\n'
        '  • remove_from_watchlist – Gỡ phim khỏi danh sách yêu thích\n'
        'Gửi vào Kafka Broker cổng 9092 với định dạng JSON: userId, movieId, action, rating, timestamp.'
    )
    kafka_code = (
        'def send_kafka_event(user_id, movie_id, action, rating=None):\n'
        '    event = {"userId": user_id, "movieId": movie_id,\n'
        '             "action": action, "rating": rating,\n'
        '             "timestamp": datetime.now().isoformat()}\n'
        '    producer.send("movie_logs", json.dumps(event).encode())\n'
        '    producer.flush()'
    )
    p_k1 = insert_after_xml(end_p8, kafka_text, style_name='Compact')
    p_k2 = insert_after_xml(p_k1, kafka_code, style_name='Code Block')
    add_image_placeholder_after(p_k2, '3.15', 'Tab Giám sát Kafka Stream Trực tiếp trên giao diện Web')
    print("   ✓ Giai đoạn 8 + placeholder hình Kafka")

# ─────────────────────────────────────────────────────────────────────────────
# 8. GIAI ĐOẠN 10 – Cập nhật Web Recommender hoàn chỉnh
# ─────────────────────────────────────────────────────────────────────────────
print("8. Cập nhật Giai đoạn 10 (Web Recommender)...")
_, p_g10 = find_para('Giai đoạn 10')
if p_g10:
    replace_para_text(p_g10,
        '10. Giai đoạn 10: Xây dựng ứng dụng Web Recommender Netflix-style hoàn chỉnh (Hoàn thành 100%)')

    end_p10 = p_g10
    for i, para in enumerate(doc.paragraphs):
        if para is p_g10:
            for j in range(i+1, len(doc.paragraphs)):
                next_p = doc.paragraphs[j]
                if 'Heading' in next_p.style.name and next_p is not p_g10:
                    break
                end_p10 = next_p
            break

    web_text = (
        'Ứng dụng Web Recommender đã hoàn thiện với đầy đủ tính năng chuẩn Netflix:\n'
        '  • Giao diện Netflix Dark Mode chuyên nghiệp với lưới phim và banner động.\n'
        '  • Tìm kiếm Autocomplete tức thì trong 62.423 bộ phim.\n'
        '  • Poster phim HD chính thức từ TMDb API kèm fallback SVG chất lượng cao.\n'
        '  • Đánh giá sao tương tác (1★ – 5★) kích hoạt gợi ý tức thời.\n'
        '  • Tab ❤️ Phim Yêu Thích (Watchlist) quản lý danh sách phim cá nhân.\n'
        '  • Tab 📡 Giám sát Kafka Stream Trực tiếp: số event, hành động, rating và trạng thái kết nối.'
    )
    p_w = insert_after_xml(end_p10, web_text, style_name='Compact')
    ref_for_11 = add_image_placeholder_after(p_w, '3.14a', 'Giao diện Web Recommender Netflix-style tại localhost:8000')
    print("   ✓ Giai đoạn 10 + placeholder hình Web Home")

# ─────────────────────────────────────────────────────────────────────────────
# 9. MỤC 11 (MỚI) – Hybrid Recommender Engine
# ─────────────────────────────────────────────────────────────────────────────
print("9. Bổ sung Mục 11 – Hybrid Recommender Engine...")
h11 = insert_heading_after_xml(ref_for_11,
    '11. Giai đoạn 11 (Mới): Động cơ gợi ý Hybrid – Franchise-Aware & Contrast Strategy', level=2)
p_d11 = insert_after_xml(h11,
    'Động cơ gợi ý Hybrid tự động chọn chế độ phù hợp dựa trên điểm đánh giá của người dùng:',
    style_name='First Paragraph')
p_sim = insert_after_xml(p_d11,
    'Chế độ 1 – Similar Mode (3★ – 5★): Ưu tiên gợi ý phim cùng vũ trụ/franchise.\n'
    '  • Thuật toán extract_franchise_root tự động bóc tách tiền tố thương hiệu '
    '(Star Wars, The Matrix, Toy Story, Lord of the Rings, Avengers...).\n'
    '  • Ưu tiên các phần tiếp theo/tiền truyện với độ hợp 98% – 99.4%.\n'
    '  • Kết hợp vector ẩn ALS và độ giao thoa thể loại Jaccard Similarity.',
    style_name='Compact')
p_con = insert_after_xml(p_sim,
    'Chế độ 2 – Contrast Mode / Đổi khẩu vị (1★ – 2★): Hệ thống đổi hướng gợi ý hoàn toàn.\n'
    '  • Loại bỏ 100% phim cùng franchise và thể loại trùng với phim bị chê.\n'
    '  • Chuyển hướng sang kiệt tác thể loại đối lập (The Shawshank Redemption, The Godfather, Pulp Fiction...).\n'
    '  • Đảm bảo danh sách gợi ý khi đánh 1★ thay đổi 100% so với 5★.',
    style_name='Compact')
p_5star = add_image_placeholder_after(p_con, '3.15a', 'Kết quả gợi ý Star Wars 5★ – Franchise-Aware Similar Mode')
ref_for_1star = p_5star  # caption của 5star
p_1star = add_image_placeholder_after(ref_for_1star, '3.15b', 'Kết quả gợi ý Star Wars 1★ – Contrast Mode (Đổi khẩu vị)')
ref_for_12 = p_1star
print("   ✓ Mục 11 đã bổ sung")

# ─────────────────────────────────────────────────────────────────────────────
# 10. MỤC 12 (MỚI) – Watchlist & Pseudo-User ALS
# ─────────────────────────────────────────────────────────────────────────────
print("10. Bổ sung Mục 12 – Watchlist & Pseudo-User ALS...")
h12 = insert_heading_after_xml(ref_for_12,
    '12. Giai đoạn 12 (Mới): Tính năng Watchlist & Thuật toán ALS Multi-Item Pseudo-User Profile', level=2)
p_d12 = insert_after_xml(h12,
    'Tính năng Watchlist giải quyết triệt để vấn đề Cold-Start: '
    'bất kỳ người dùng nào có thể nhận gợi ý cá nhân hóa mà không cần đăng nhập hay có lịch sử trước đó.',
    style_name='First Paragraph')
p_wl = insert_after_xml(p_d12,
    'Luồng hoạt động:\n'
    '  1. Người dùng bấm ❤️ Yêu thích trên bất kỳ thẻ phim nào.\n'
    '  2. Tab ❤️ Phim Yêu Thích hiển thị danh sách kèm nút Bỏ lưu.\n'
    '  3. Nút 🔮 GỢI Ý TỪ TOÀN BỘ PHIM YÊU THÍCH kích hoạt thuật toán:\n'
    '     a. Trích xuất vector ẩn ALS vm của từng phim trong Watchlist W từ tầng Gold HDFS.\n'
    '     b. Tính Pseudo-User Latent Vector:\n'
    '        u_pseudo = (1/|W|) × Σ vm,   û = u_pseudo / ‖u_pseudo‖\n'
    '     c. Cosine Similarity(û, vc) với mọi phim ứng viên, kết hợp Franchise Boost + Genre Affinity.\n'
    '     d. Tự động lọc bỏ phim đã có trong Watchlist.',
    style_name='Compact')
p_wl_img = add_image_placeholder_after(p_wl, '3.16a', 'Tab Phim Yêu Thích (Watchlist) trên giao diện Web')
p_als_img = add_image_placeholder_after(p_wl_img, '3.16b',
    'Kết quả gợi ý ALS Pseudo-User từ toàn bộ phim trong Watchlist')
ref_for_13 = p_als_img
print("   ✓ Mục 12 đã bổ sung")

# ─────────────────────────────────────────────────────────────────────────────
# 11. MỤC 13 (MỚI) – Tự động hóa 1-Click
# ─────────────────────────────────────────────────────────────────────────────
print("11. Bổ sung Mục 13 – Tự động hóa 1-Click...")
h13 = insert_heading_after_xml(ref_for_13,
    '13. Giai đoạn 13 (Mới): Tự động hóa 1-Click Khởi động & Vận hành Hệ thống', level=2)
p_d13 = insert_after_xml(h13,
    'Bộ kịch bản tự động hóa đảm bảo tính dễ vận hành và khả năng tái tạo môi trường trên cả '
    'Windows (START_SYSTEM.bat / START_SYSTEM.ps1) và Linux/WSL2 (start_all.sh):',
    style_name='First Paragraph')
p_sc = insert_after_xml(p_d13,
    'Quy trình khởi động 4 bước:\n'
    '  1. Khởi động cụm Hadoop HDFS (NameNode + DataNode 1).\n'
    '  2. Khởi động DataNode 2 độc lập (Cluster 2 Live Nodes).\n'
    '  3. Khởi động Apache Kafka KRaft Broker (cổng 9092) + topic movie_logs.\n'
    '  4. Kích hoạt FastAPI Uvicorn (cổng 8000) và mở trình duyệt tự động.\n\n'
    'STOP_SYSTEM.bat / stop_all.sh: Dừng an toàn mọi tiến trình theo thứ tự ngược lại.',
    style_name='Compact')
add_image_placeholder_after(p_sc, '3.17',
    'Cửa sổ START_SYSTEM.bat – 4/4 dịch vụ khởi động thành công')
print("   ✓ Mục 13 đã bổ sung")

# ─────────────────────────────────────────────────────────────────────────────
# 12. BẢNG 6 – Tiến độ: cập nhật trạng thái + thêm hàng 11, 12, 13
# ─────────────────────────────────────────────────────────────────────────────
print("12. Cập nhật Bảng 6 (Tiến độ)...")
table6 = doc.tables[6]
for row in table6.rows[1:]:
    for cell in row.cells:
        for para in cell.paragraphs:
            t = para.text.strip()
            if t in ('Hoàn thành mức cơ bản', 'Thử nghiệm', 'Đang thực hiện', 'Chưa bắt đầu'):
                replace_para_text(para, 'Hoàn thành')
            if para.text.strip() not in ('', '100%', 'Hoàn thành') and cell is row.cells[-1]:
                if para.text.strip().endswith('%') and para.text.strip() != '100%':
                    replace_para_text(para, '100%')

add_table_row(table6, ['11',
    'Động cơ gợi ý Hybrid Franchise-Aware & Contrast Strategy',
    'recommend_engine.py', 'FastAPI, NumPy, Cosine Similarity', 'Hoàn thành', '100%'])
add_table_row(table6, ['12',
    'Mục Phim Yêu Thích (Watchlist) & Gợi ý ALS Pseudo-User Vector',
    'main.py (Watchlist API)', 'FastAPI, HDFS, ALS itemFactors', 'Hoàn thành', '100%'])
add_table_row(table6, ['13',
    'Tự động hóa 1-Click khởi động toàn cụm phân tán',
    'START_SYSTEM.bat, start_all.sh', 'Bash, PowerShell, Windows BAT', 'Hoàn thành', '100%'])
print("   ✓ Bảng 6 đã cập nhật")

# ─────────────────────────────────────────────────────────────────────────────
# 13. BẢNG 7 – Thêm ghi chú cột cuối kỳ
# ─────────────────────────────────────────────────────────────────────────────
print("13. Cập nhật Bảng 7...")
table7 = doc.tables[7]
if len(table7.rows) > 0:
    last_cell = table7.rows[0].cells[-1]
    if 'Cuối kỳ' not in last_cell.text:
        last_cell.paragraphs[0].add_run('\n→ Bao gồm kết quả Hoàn thiện Cuối kỳ')
print("   ✓ Bảng 7 đã cập nhật")

# ─────────────────────────────────────────────────────────────────────────────
# 14. MỤC 4 CHƯƠNG IV – Chuyển hạn chế → Kết quả giải quyết
# ─────────────────────────────────────────────────────────────────────────────
print("14. Cập nhật Mục 4 (Kết quả giải quyết hạn chế)...")
_, p_muc4 = find_para('Các phần chưa hoàn thành')
if p_muc4:
    replace_para_text(p_muc4,
        '4. Kết quả giải quyết các hạn chế kỹ thuật đã nêu ở báo cáo tiến độ')
    print("   ✓ Đổi tiêu đề Mục 4")

# Đổi tiêu đề sub-heading
_, p_sub = find_para('Các hạn chế kỹ thuật hiện tại')
if p_sub:
    replace_para_text(p_sub, 'Danh sách hạn chế đã giải quyết:')

# Cập nhật từng điểm hạn chế
resolutions = {
    'Clipping': '✅ Điểm số Clipping: Giải quyết bằng Cosine Similarity từ ALS Item Factors. '
                'Điểm luôn chuẩn hóa 0% – 100%, không còn vượt ngưỡng 5.0.',
    'Tích hợp dữ liệu streaming': '✅ Tích hợp Streaming: Mọi hành vi trên Web '
                '(rate, add_to_watchlist, remove_from_watchlist) gửi vào Kafka movie_logs với bảng theo dõi thời gian thực.',
    'Airflow DAG còn ở mức đơn nhiệm': '✅ Airflow DAG: Vận hành thành công lịch trình tự động hóa hàng ngày.',
    'Giao diện Web mới ở mức Dashboard': '✅ Giao diện Web: Đã hoàn thiện Web App tương tác cao, '
                'tra cứu 62k phim, TMDb Poster, đánh giá sao trực tiếp.',
    'cùng bộ': '✅ Gợi ý phim cùng franchise (Star Wars, Matrix...): '
               'Giải quyết bằng Franchise Root Extraction + Scoring Boost.',
    'Đánh giá 1': '✅ Đánh giá 1★ vs 5★: Giải quyết bằng Contrast Strategy, '
                  'đổi khẩu vị 100% sang kiệt tác thể loại đối lập.',
    'Cold-Start': '✅ Cold-Start & Gợi ý đa phim: Giải quyết bằng Watchlist + '
                  'Pseudo-User Latent Profile Vector.',
}
for para in doc.paragraphs:
    if 'Heading' in para.style.name:
        continue
    for kw, res in resolutions.items():
        if kw.lower() in para.text.lower() and '✅' not in para.text:
            replace_para_text(para, res)
            break

# Đổi tiêu đề "Kế hoạch hành động"
_, p_kh = find_para('Kế hoạch hành động')
if p_kh:
    replace_para_text(p_kh, 'Tổng kết: Mọi hạn chế kỹ thuật đã được khắc phục hoàn toàn trong giai đoạn cuối kỳ.')
print("   ✓ Mục 4 đã cập nhật")

# ─────────────────────────────────────────────────────────────────────────────
# 15. BẢNG 8 – Chuyển kế hoạch → kết quả
# ─────────────────────────────────────────────────────────────────────────────
print("15. Cập nhật Bảng 8 (Kế hoạch → Kết quả)...")
table8 = doc.tables[8]
# Tiêu đề caption
_, p_b8_cap = find_para('Kế hoạch chi tiết triển khai giai đoạn 2')
if not p_b8_cap:
    _, p_b8_cap = find_para('Bảng 8')
if p_b8_cap:
    replace_para_text(p_b8_cap, 'Bảng 8. Kết quả triển khai và hoàn thiện đồ án giai đoạn cuối kỳ')

if len(table8.rows) > 0:
    for cell in table8.rows[0].cells:
        if 'dự kiến' in cell.text.lower():
            replace_para_text(cell.paragraphs[0], 'Kết quả đạt được (Hoàn thành 100%)')
            break

completions = {
    'ALS': 'Hoàn thành. RMSE=0.817. Cosine Similarity chuẩn hóa 0–100%.',
    'Kafka': 'Hoàn thành. Kafka KRaft đa sự kiện (rate, add_to_watchlist, remove_from_watchlist).',
    'Giao diện': 'Hoàn thành. Web Netflix-style, 62.423 phim, TMDb Poster, đánh giá sao.',
    'Watchlist': 'Hoàn thành. Watchlist + Pseudo-User ALS Profile Vector.',
    'Tự động': 'Hoàn thành. START_SYSTEM.bat / start_all.sh – 1-click khởi động 4/4 dịch vụ.',
    'Airflow': 'Hoàn thành. DAG movie_data_pipeline vận hành tự động hóa hàng ngày.',
}
for row in table8.rows[1:]:
    for kw, val in completions.items():
        if kw.lower() in row.cells[1].text.lower():
            if len(row.cells) >= 3:
                replace_para_text(row.cells[2].paragraphs[0], val)
            break
print("   ✓ Bảng 8 đã cập nhật")

# ─────────────────────────────────────────────────────────────────────────────
# 16. KẾT LUẬN – Cập nhật + bổ sung điểm mới
# ─────────────────────────────────────────────────────────────────────────────
print("16. Cập nhật Kết luận...")
_, p_kl_head = find_para('KẾT LUẬN', style='Heading 1')
if p_kl_head:
    for i, para in enumerate(doc.paragraphs):
        if para is p_kl_head:
            for j in range(i+1, min(i+4, len(doc.paragraphs))):
                p = doc.paragraphs[j]
                if 'tiến độ' in p.text.lower() and len(p.text) > 50:
                    replace_para_text(p,
                        'Đến thời điểm hoàn thiện đồ án, nhóm đã hoàn thành 100% tất cả các thành phần '
                        'của hệ thống Gợi ý Phim trực tuyến dựa trên kiến trúc Lakehouse và điện toán phân tán:')
                    break

    # Tìm đoạn cuối kết luận
    last_bullet = None
    for i, para in enumerate(doc.paragraphs):
        if para is p_kl_head:
            for j in range(i+1, len(doc.paragraphs)):
                p = doc.paragraphs[j]
                if 'Heading' in p.style.name and j > i+1:
                    break
                if p.style.name == 'Compact':
                    last_bullet = p
            break

    if last_bullet:
        new_bullets = [
            'Hoàn thiện động cơ gợi ý Hybrid thông minh: Franchise-Aware Similar Mode (3★–5★) '
            'và Contrast Mode (1★–2★) đảm bảo trải nghiệm phù hợp cảm xúc người dùng.',
            'Watchlist và thuật toán Pseudo-User Latent Profile Vector giải quyết triệt để Cold-Start, '
            'không yêu cầu đăng nhập hay lịch sử trước đó.',
            'Luồng Streaming Kafka thời gian thực 2 chiều với đa sự kiện (rate, add_to_watchlist, '
            'remove_from_watchlist), hiển thị giám sát trực tiếp trên Web.',
            'Kịch bản tự động hóa 1-click (START_SYSTEM.bat, start_all.sh) khởi động toàn cụm '
            'HDFS 2 DataNodes, Kafka KRaft và FastAPI nhất quán, dễ vận hành.',
        ]
        ref = last_bullet
        for bullet in new_bullets:
            ref = insert_after_xml(ref, bullet, style_name='Compact')

    # Cập nhật đoạn kết
    for para in doc.paragraphs:
        if 'giai đoạn tiếp' in para.text.lower() or 'sẽ tiếp tục' in para.text.lower():
            replace_para_text(para,
                'Đề tài có tính ứng dụng thực tiễn cao với giao diện Netflix-style hiện đại, '
                'động cơ gợi ý Hybrid thông minh, tính năng Watchlist giải quyết Cold-Start và '
                'quy trình vận hành tự động hóa 1-click. Đây là minh chứng thực tế cho khả năng ứng dụng '
                'công nghệ Big Data – Lakehouse vào bài toán gợi ý phim cá nhân hóa quy mô lớn.')
            break

print("   ✓ Kết luận đã cập nhật")

# ─────────────────────────────────────────────────────────────────────────────
# LƯU FILE
# ─────────────────────────────────────────────────────────────────────────────
print(f"\nĐang lưu → {DEST}")
doc.save(DEST)
print("=" * 60)
print("✅ HOÀN TẤT! File đã lưu thành công.")
print(f"   → {DEST}")
