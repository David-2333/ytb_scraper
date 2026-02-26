import tkinter as tk
from tkinter import ttk, messagebox
import threading
import json
import os
import time
import re
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import isodate
import queue
import concurrent.futures

CONFIG_FILE = "yt_keys_config.json"

# ==========================================
# 企业级 API 负载均衡器 (完全线程安全 + 智能网络防抖)
# ==========================================
class ThreadSafeAPIKeyPool:
    def __init__(self, api_keys, log_callback):
        self.lock = threading.Lock()
        self.log_callback = log_callback
        
        if not api_keys:
            raise Exception("未检测到 API Key，请先在设置中添加。")
            
        self.api_keys = list(api_keys)
        self.index = 0
        self.local_data = threading.local()
        
        self.log_callback(f"✅ 负载均衡器启动，挂载 {len(self.api_keys)} 个可用 Key。")

    def get_client(self):
        with self.lock:
            if not self.api_keys:
                raise Exception("❌ 严重错误: 所有 API Key 配额均已耗尽！")
            key = self.api_keys[self.index % len(self.api_keys)]
            self.index += 1
            
        if not hasattr(self.local_data, 'clients'):
            self.local_data.clients = {}
            
        if key not in self.local_data.clients:
            self.local_data.clients[key] = build('youtube', 'v3', developerKey=key, cache_discovery=False)
            
        return self.local_data.clients[key], key

    def mark_exhausted(self, key):
        with self.lock:
            if key in self.api_keys:
                self.api_keys.remove(key)
                self.log_callback(f"⚠️ [熔断机制] Key {key[:5]}... 配额耗尽！剩余可用: {len(self.api_keys)} 个")

    def execute(self, api_func, max_retries=4):
        for attempt in range(max_retries):
            client, key = self.get_client()
            try:
                return api_func(client)
            except HttpError as e:
                if e.resp.status == 403:
                    err_str = e.content.decode("utf-8").lower()
                    if "quotaexceeded" in err_str or "ratelimitexceeded" in err_str:
                        self.mark_exhausted(key)
                        continue 
                    else:
                        raise e
                elif e.resp.status in[500, 502, 503, 504]:
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                raise e
            except Exception as e:
                err_str = str(e).lower()
                if any(x in err_str for x in["ssl", "incomplete", "version", "decryption", "mac", "timeout", "connection", "reset"]):
                    if attempt < max_retries - 1:
                        short_err = str(e).split('] ')[-1][:35] if ']' in str(e) else str(e)[:35]
                        self.log_callback(f"⚠️ 网络防抖 ({short_err})... 自动重试 ({attempt+1}/{max_retries})")
                        time.sleep(1 + attempt)
                        continue
                raise e
        raise Exception("网络连续请求失败超过上限，放弃当前请求。")

# ==========================================
# 图形化界面与主业务逻辑
# ==========================================
class YouTubeScraperApp:
    def __init__(self, root):
        self.root = root
        self.root.title("YouTube 全能数据爬取控制台0.8 Beta (Designed by David and Gemini)")
        self.root.geometry("820x800")
        
        self.api_keys = self.load_keys()
        
        self.current_channel_id = None
        self.uploads_playlist_id = None
        self.current_video_id = None
        self.verified_video_title = None
        self.verified_video_published_at = None
        
        self.msg_queue = queue.Queue()
        
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(expand=True, fill='both', padx=10, pady=10)
        
        self.tab_main = ttk.Frame(self.notebook)
        self.tab_settings = ttk.Frame(self.notebook)
        
        self.notebook.add(self.tab_main, text="主页")
        self.notebook.add(self.tab_settings, text="API Key 设置")
        
        self.setup_main_tab()
        self.setup_settings_tab()
        self.root.after(100, self.process_queue)

    def load_keys(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f: return json.load(f)
            except: return []
        return[]

    def save_keys(self):
        with open(CONFIG_FILE, 'w') as f: json.dump(self.api_keys, f)

    def extract_video_id(self, url_or_id):
        pattern = r'(?:v=|\/)([0-9A-Za-z_-]{11}).*'
        match = re.search(pattern, url_or_id)
        if match: return match.group(1)
        if len(url_or_id.strip()) == 11: return url_or_id.strip()
        return None

    def on_mode_change(self):
        mode = self.scrape_mode.get()
        self.lbl_verify_status.config(text="")
        self.current_channel_id = None
        self.current_video_id = None
        
        if mode == "channel":
            self.lbl_target_hint.config(text="输入频道ID或Handle (如 @YouTube 或 UCAu...):")
            self.time_entry.config(state='normal')
        else:
            self.lbl_target_hint.config(text="输入视频链接或视频ID (如 https://youtube.com/watch?v=...):")
            self.time_entry.config(state='disabled')

    def setup_main_tab(self):
        # 1. 爬取目标设置
        frame_target = ttk.LabelFrame(self.tab_main, text="1. 爬取目标设置")
        frame_target.pack(fill='x', padx=10, pady=5)
        
        self.scrape_mode = tk.StringVar(value="channel")
        frame_mode = ttk.Frame(frame_target)
        frame_mode.pack(anchor='w', padx=5, pady=2)
        ttk.Radiobutton(frame_mode, text="按频道爬取 (抓取该频道下多期视频)", variable=self.scrape_mode, value="channel", command=self.on_mode_change).pack(side='left', padx=15)
        ttk.Radiobutton(frame_mode, text="单一视频爬取 (仅抓取指定单个视频)", variable=self.scrape_mode, value="video", command=self.on_mode_change).pack(side='left', padx=15)

        self.lbl_target_hint = ttk.Label(frame_target, text="输入频道ID或Handle (如 @YouTube 或 UCAu...):")
        self.lbl_target_hint.pack(anchor='w', padx=5, pady=2)
        
        frame_input = ttk.Frame(frame_target)
        frame_input.pack(fill='x', padx=5, pady=2)
        self.target_entry = ttk.Entry(frame_input, width=50)
        self.target_entry.pack(side='left', padx=5, pady=5)
        self.btn_verify = ttk.Button(frame_input, text="验证目标", command=self.verify_target)
        self.btn_verify.pack(side='left', padx=5, pady=5)
        self.lbl_verify_status = ttk.Label(frame_input, text="")
        self.lbl_verify_status.pack(side='left', padx=5, pady=5)

        # 2. 数据选项区域
        frame_options = ttk.LabelFrame(self.tab_main, text="2. 选择需要爬取的常规数据")
        frame_options.pack(fill='x', padx=10, pady=5)
        self.var_title = tk.BooleanVar(value=True)
        self.var_time = tk.BooleanVar(value=True)
        self.var_duration = tk.BooleanVar(value=True)
        self.var_views = tk.BooleanVar(value=True)
        self.var_likes = tk.BooleanVar(value=True)
        self.var_comments = tk.BooleanVar(value=True)
        
        opts =[("视频标题", self.var_title), ("发布时间", self.var_time), 
                ("视频时长", self.var_duration), ("观看量", self.var_views), 
                ("点赞量", self.var_likes), ("开启评论抓取", self.var_comments)]
        for i, (text, var) in enumerate(opts):
            ttk.Checkbutton(frame_options, text=text, variable=var).grid(row=i//3, column=i%3, padx=15, pady=5, sticky='w')

        # 3. 评论高级控制区域
        frame_comment_opts = ttk.LabelFrame(self.tab_main, text="3. 评论高级控制")
        frame_comment_opts.pack(fill='x', padx=10, pady=5)
        ttk.Label(frame_comment_opts, text="单视频评论上限:").grid(row=0, column=0, padx=5, pady=5, sticky='e')
        self.var_comment_limit = ttk.Combobox(frame_comment_opts, values=["无限制", "50", "100", "500", "1000", "5000", "10000"], width=10)
        self.var_comment_limit.set("500")
        self.var_comment_limit.grid(row=0, column=1, padx=5, pady=5, sticky='w')
        ttk.Label(frame_comment_opts, text="排序规则:").grid(row=0, column=2, padx=15, pady=5, sticky='e')
        self.var_comment_order = ttk.Combobox(frame_comment_opts, values=["按热度排序 (高赞优先)", "按时间排序 (最新优先)"], width=25)
        self.var_comment_order.set("按热度排序 (高赞优先)")
        self.var_comment_order.grid(row=0, column=3, padx=5, pady=5, sticky='w')

        # 4. 时间与执行区域
        frame_time = ttk.LabelFrame(self.tab_main, text="4. 爬取时间范围 (0-5年(1825天))")
        frame_time.pack(fill='x', padx=10, pady=5)
        ttk.Label(frame_time, text="爬取过去").pack(side='left', padx=5)
        self.time_entry = ttk.Entry(frame_time, width=10)
        self.time_entry.insert(0, "30")
        self.time_entry.pack(side='left', padx=5)
        ttk.Label(frame_time, text="天内发布的视频").pack(side='left', padx=5)

        frame_exec = ttk.Frame(self.tab_main)
        frame_exec.pack(fill='both', expand=True, padx=10, pady=5)
        self.btn_start = ttk.Button(frame_exec, text="🔥 开始智能爬取并导出 Excel", command=self.start_scraping)
        self.btn_start.pack(pady=5)
        self.progress = ttk.Progressbar(frame_exec, orient='horizontal', mode='determinate')
        self.progress.pack(fill='x', pady=5)
        self.log_text = tk.Text(frame_exec, height=10, state='disabled', bg="#f4f4f4")
        self.log_text.pack(fill='both', expand=True, pady=5)

    def setup_settings_tab(self):
        frame_keys = ttk.LabelFrame(self.tab_settings, text="YouTube Data API v3 密钥池")
        frame_keys.pack(fill='both', expand=True, padx=10, pady=10)
        self.key_listbox = tk.Listbox(frame_keys, width=80, height=10)
        self.key_listbox.pack(padx=10, pady=10)
        for key in self.api_keys: self.key_listbox.insert(tk.END, key)
        frame_add = ttk.Frame(frame_keys)
        frame_add.pack(fill='x', padx=10, pady=5)
        self.key_entry = ttk.Entry(frame_add, width=50)
        self.key_entry.pack(side='left', padx=5)
        ttk.Button(frame_add, text="添加 Key", command=self.add_key).pack(side='left', padx=5)
        ttk.Button(frame_add, text="删除选中", command=self.remove_key).pack(side='left', padx=5)

    def add_key(self):
        key = self.key_entry.get().strip()
        if key and key not in self.api_keys:
            self.api_keys.append(key)
            self.key_listbox.insert(tk.END, key)
            self.save_keys()
            self.key_entry.delete(0, tk.END)

    def remove_key(self):
        selected = self.key_listbox.curselection()
        if selected:
            idx = selected[0]
            del self.api_keys[idx]
            self.key_listbox.delete(idx)
            self.save_keys()

    def log(self, message):
        self.msg_queue.put({"type": "log", "msg": message})

    def process_queue(self):
        while not self.msg_queue.empty():
            msg = self.msg_queue.get()
            if msg["type"] == "log":
                self.log_text.config(state='normal')
                self.log_text.insert(tk.END, msg["msg"] + "\n")
                self.log_text.see(tk.END)
                self.log_text.config(state='disabled')
            elif msg["type"] == "progress":
                self.progress['value'] = msg["val"]
                self.progress['maximum'] = msg["max"]
            elif msg["type"] == "done":
                self.btn_start.config(state='normal')
                messagebox.showinfo("完成", f"✅ 爬取完成！\n数据已保存至:\n{msg['path']}")
            elif msg["type"] == "error":
                self.btn_start.config(state='normal')
                messagebox.showerror("错误/中断", msg["msg"])
        self.root.after(100, self.process_queue)

    def verify_target(self):
        identifier = self.target_entry.get().strip()
        if not identifier: return messagebox.showwarning("提示", "请输入目标标识")
        
        mode = self.scrape_mode.get()
        try:
            pool = ThreadSafeAPIKeyPool(self.api_keys, lambda x: None)
            
            if mode == "channel":
                def fetch_channel(client):
                    if identifier.startswith('@'):
                        return client.channels().list(part="snippet,contentDetails", forHandle=identifier).execute()
                    else:
                        return client.channels().list(part="snippet,contentDetails", id=identifier).execute()

                res = pool.execute(fetch_channel)
                if not res.get('items'):
                    self.lbl_verify_status.config(text="❌ 未找到该频道", foreground="red")
                    self.current_channel_id = None
                else:
                    channel = res['items'][0]
                    self.current_channel_id = channel['id']
                    self.uploads_playlist_id = channel['contentDetails']['relatedPlaylists']['uploads']
                    self.lbl_verify_status.config(text=f"✅ {channel['snippet']['title']}", foreground="green")
            
            elif mode == "video":
                vid = self.extract_video_id(identifier)
                if not vid:
                    self.lbl_verify_status.config(text="❌ 无法识别有效的视频ID或链接", foreground="red")
                    self.current_video_id = None
                    return
                    
                def fetch_video(client):
                    return client.videos().list(part="snippet", id=vid).execute()
                    
                res = pool.execute(fetch_video)
                if not res.get('items'):
                    self.lbl_verify_status.config(text="❌ 未找到该视频", foreground="red")
                    self.current_video_id = None
                else:
                    video = res['items'][0]
                    self.current_video_id = video['id']
                    title = video['snippet']['title']
                    self.verified_video_title = title
                    self.verified_video_published_at = video['snippet']['publishedAt']
                    display_title = title if len(title) <= 25 else title[:25] + "..."
                    self.lbl_verify_status.config(text=f"✅ {display_title}", foreground="green")

        except Exception as e:
            self.lbl_verify_status.config(text="❌ 验证失败(检查网络或API Key)", foreground="red")

    def start_scraping(self):
        mode = self.scrape_mode.get()
        if mode == "channel" and not self.current_channel_id: 
            return messagebox.showwarning("提示", "请先验证并确认频道存在！")
        if mode == "video" and not self.current_video_id: 
            return messagebox.showwarning("提示", "请先验证并确认视频存在！")
        if not self.api_keys: 
            return messagebox.showwarning("提示", "请至少添加一个 API Key！")
        
        days = 30
        if mode == "channel":
            try:
                days = int(self.time_entry.get())
                if not (3 <= days <= 1825): raise ValueError
            except:
                return messagebox.showwarning("提示", "时间范围必须在3-1825天之间！")

        scrape_config = {
            'title': self.var_title.get(),
            'time': self.var_time.get(),
            'duration': self.var_duration.get(),
            'views': self.var_views.get(),
            'likes': self.var_likes.get(),
            'comments': self.var_comments.get(),
        }

        order_param = "relevance" if "热度" in self.var_comment_order.get() else "time"
        limit_str = self.var_comment_limit.get()
        comment_limit = float('inf') if limit_str == "无限制" else int(limit_str)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

        self.btn_start.config(state='disabled')
        self.progress['value'] = 0
        self.log_text.config(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state='disabled')
        
        threading.Thread(target=self.run_scraping_task, args=(cutoff_date, comment_limit, order_param, scrape_config, mode), daemon=True).start()

    def run_scraping_task(self, cutoff_date, comment_limit, order_param, scrape_config, mode):
        self.log(f"🚀 初始化爬取引擎 (当前模式: {'频道多视频' if mode == 'channel' else '单一精细视频'}) ...")
        try:
            pool = ThreadSafeAPIKeyPool(self.api_keys, self.log)
            videos =[]
            
            if mode == "channel":
                next_page = None
                self.log("正在拉取频道的视频列表...")
                while True:
                    def get_playlist(client, token=next_page):
                        return client.playlistItems().list(
                            part="snippet", playlistId=self.uploads_playlist_id,
                            maxResults=50, pageToken=token
                        ).execute()
                        
                    res = pool.execute(get_playlist)
                    out_of_range = False
                    for item in res.get('items',[]):
                        pub_time = datetime.fromisoformat(item['snippet']['publishedAt'].replace('Z', '+00:00'))
                        if pub_time < cutoff_date:
                            out_of_range = True; break 
                        videos.append({
                            'video_id': item['snippet']['resourceId']['videoId'],
                            'title': item['snippet']['title'],
                            'published_at': item['snippet']['publishedAt']
                        })
                    
                    next_page = res.get('nextPageToken')
                    if not next_page or out_of_range: break
            else:
                videos.append({
                    'video_id': self.current_video_id,
                    'title': self.verified_video_title,
                    'published_at': self.verified_video_published_at
                })
                    
            if not videos: return self.msg_queue.put({"type": "error", "msg": "未找到符合要求的视频。"})
            self.log(f"✅ 共 {len(videos)} 个视频等待处理，开始提取详细数据...")

            final_data =[]
            max_workers = min(len(pool.api_keys) * 4, 15) 
            self.msg_queue.put({"type": "progress", "val": 0, "max": len(videos)})
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.process_single_video, v, pool, comment_limit, order_param, scrape_config): v for v in videos}
                completed = 0
                for future in concurrent.futures.as_completed(futures):
                    try:
                        final_data.extend(future.result())
                    except Exception as e:
                        self.log(f"❌ 视频处理异常被跳过: {e}")
                    completed += 1
                    self.msg_queue.put({"type": "progress", "val": completed, "max": len(videos)})
                    self.log(f"⏳ 进度: {completed}/{len(videos)} 个视频已处理完毕。")
                    
            if final_data:
                df = pd.DataFrame(final_data)
                prefix = "YouTube_Channel" if mode == "channel" else "YouTube_SingleVideo"
                filename = f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                filepath = os.path.join(os.path.expanduser('~'), 'Downloads', filename)
                
                df.to_excel(filepath, index=False)
                self.msg_queue.put({"type": "done", "path": filepath})
            else:
                self.msg_queue.put({"type": "error", "msg": "未能提取到有效数据。"})

        except Exception as e:
            self.msg_queue.put({"type": "error", "msg": str(e)})

    def process_single_video(self, video_info, pool, comment_limit, order_param, config):
        vid = video_info['video_id']
        row_base = {}
        
        if config['title']: row_base['视频标题'] = video_info['title']
        if config['time']: row_base['发布时间'] = video_info['published_at']
        
        if config['duration'] or config['views'] or config['likes']:
            try:
                def get_stats(client):
                    return client.videos().list(part="contentDetails,statistics", id=vid).execute()
                
                stats_res = pool.execute(get_stats)
                if stats_res.get('items'):
                    info = stats_res['items'][0]
                    if config['duration']:
                        dur_iso = info.get('contentDetails', {}).get('duration', 'PT0S')
                        row_base['时长(秒)'] = int(isodate.parse_duration(dur_iso).total_seconds())
                    if config['views']:
                        row_base['观看量'] = int(info.get('statistics', {}).get('viewCount', 0))
                    if config['likes']:
                        row_base['点赞量'] = int(info.get('statistics', {}).get('likeCount', 0))
            except Exception as e:
                self.log(f"⚠️ 视频 {vid} 播放/点赞数据拉取失败: {str(e)[:30]}")
                
        results = []
        if config['comments']:
            fetched_count = 0
            next_page = None
            try:
                while True:
                    remaining = comment_limit - fetched_count
                    if remaining <= 0: break
                        
                    req_max = min(100, remaining)
                    def get_comments(client, token=next_page, r_max=req_max):
                        return client.commentThreads().list(
                            part="snippet", videoId=vid, maxResults=r_max,
                            pageToken=token, order=order_param, textFormat="plainText"
                        ).execute()

                    res = pool.execute(get_comments)
                    
                    # 避免没有评论时报错
                    items = res.get('items',[])
                    if not items: 
                        break # 没有获取到任何评论数据，跳出循环
                    
                    for item in items:
                        comment = item['snippet']['topLevelComment']['snippet']
                        row = row_base.copy()
                        row['评论作者'] = comment.get('authorDisplayName', '')
                        row['评论内容'] = comment.get('textDisplay', '')
                        row['评论点赞数'] = comment.get('likeCount', 0)
                        row['评论发布时间'] = comment.get('publishedAt', '')
                        results.append(row)
                        fetched_count += 1
                        if fetched_count >= comment_limit: break
                            
                    next_page = res.get('nextPageToken')
                    if not next_page or fetched_count >= comment_limit: break
                        
            except HttpError as e:
                if "disabled" in str(e).lower() or "forbidden" in str(e).lower():
                    row = row_base.copy()
                    row['评论内容'] = "[该视频已被 UP 主关闭评论区]"
                    results.append(row)
                else:
                    row = row_base.copy()
                    row['评论内容'] = f"[评论拉取异常]"
                    results.append(row)
                    
            # 🌟 修复关键：如果走完了上面的评论拉取流程，但由于没有任何评论导致 results 依旧为空
            # 我们在此处强行塞入基础数据，并加上无评论说明，避免丢失整个视频的其他数据
            if len(results) == 0:
                row = row_base.copy()
                row['评论内容'] = "[该视频暂无评论]"
                results.append(row)
                
        else:
            # 用户在界面根本没有勾选抓评论的情况
            results.append(row_base)
            
        return results

if __name__ == "__main__":
    root = tk.Tk()
    app = YouTubeScraperApp(root)
    root.mainloop()