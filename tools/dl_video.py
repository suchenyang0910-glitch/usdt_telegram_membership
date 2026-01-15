import argparse
import sys
import os

# 添加项目根目录到 path 以便导入 scrapers
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.video_dl import VideoDownloader

def main():
    parser = argparse.ArgumentParser(description="Download video using yt-dlp wrapper")
    parser.add_argument("url", help="Video URL to download")
    # 默认路径改为 E:\资源\userbot\downloads
    default_dir = r"E:\资源\userbot\downloads"
    parser.add_argument("--dir", default=default_dir, help=f"Output directory (default: {default_dir})")
    parser.add_argument("--proxy", help="HTTP/HTTPS proxy URL")
    
    args = parser.parse_args()
    
    print(f"Downloading from: {args.url}")
    print(f"Base Output dir: {args.dir}")
    
    dl = VideoDownloader(output_dir=args.dir, proxy=args.proxy)
    info = dl.download(args.url)
    
    if info:
        print("\n✅ Download Completed!")
        print(f"Folder: {info.folder_path}")
        print(f"Title:  {info.title}")
        print(f"Video:  {os.path.basename(info.file_path) if info.file_path else 'N/A'}")
        print(f"Thumb:  {os.path.basename(info.thumbnail) if info.thumbnail else 'N/A'}")
        print(f"Meta:   {os.path.basename(info.meta_path) if info.meta_path else 'N/A'}")
        if info.categories:
            print(f"Cats:   {', '.join(info.categories)}")
        if info.tags:
            print(f"Tags:   {', '.join(info.tags[:5])}...")
    else:
        print("\n❌ Download Failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
