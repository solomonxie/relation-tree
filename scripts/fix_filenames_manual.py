
import os

TXT_DIR = "blobs/qq_txt"

# Manual fix for remaining unknowns
MANUAL_FIX = {
    "sender_id_unknown__sender_name_大纪__receiver_name_几何体.txt": "sender_id_dj_9406__sender_name_大纪__receiver_name_几何体.txt",
    "sender_id_unknown__sender_name_忘不了的好鸟__receiver_name_几何体.txt": "sender_id_group_forgotten_birds__sender_name_忘不了的好鸟__receiver_name_几何体.txt",
    "sender_id_unknown__sender_name_格物致知__receiver_name_几何体.txt": "sender_id_group_gewu__sender_name_格物致知__receiver_name_几何体.txt",
    "sender_id_unknown__sender_name_王京广__receiver_name_几何体.txt": "sender_id_xiaozi_wjg__sender_name_王京广__receiver_name_几何体.txt",
    "sender_id_unknown__sender_name_北工商金融2011届__receiver_name_几何体.txt": "sender_id_group_btbu_2011__sender_name_北工商金融2011届__receiver_name_几何体.txt",
    "sender_id_unknown__sender_name_第五组nbsp论文__receiver_name_几何体.txt": "sender_id_group_paper_5__sender_name_第五组论文__receiver_name_几何体.txt",
    "sender_id_unknown__sender_name_wiwere零班__receiver_name_几何体_1.txt": "sender_id_group_wiwere_0__sender_name_wiwere零班__receiver_name_几何体_1.txt",
    "sender_id_unknown__sender_name_wiwere零班__receiver_name_几何体.txt": "sender_id_group_wiwere_0__sender_name_wiwere零班__receiver_name_几何体.txt",
}

def main():
    for old, new in MANUAL_FIX.items():
        old_path = os.path.join(TXT_DIR, old)
        new_path = os.path.join(TXT_DIR, new)
        if os.path.exists(old_path):
            os.rename(old_path, new_path)
            print(f"Manual Rename: {old} -> {new}")

if __name__ == "__main__":
    main()
