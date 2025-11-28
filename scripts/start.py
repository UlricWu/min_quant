from src.data.ftp_downloader import FTPDownloader

if __name__ == "__main__":
    downloader = FTPDownloader()
    downloader.download("2025-08-29")   # 或 downloader.download() 下载今日



