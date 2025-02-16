from telethon import TelegramClient
import os
import asyncio
from dotenv import load_dotenv
from tqdm import tqdm

# Load .env file
load_dotenv()
api_id = os.getenv("api_id")
api_hash = os.getenv("api_hash")
phone_number = os.getenv("phone_number")

RETRY_DELAY = 3  # seconds
MAX_RETRIES = 3
DOWNLOADS_FOLDER = "downloads"  # Define downloads folder

class DownloadManager:
    def __init__(self, client: TelegramClient):
        self.client = client
        # Create downloads directory if it doesn't exist
        os.makedirs(DOWNLOADS_FOLDER, exist_ok=True)
        
    async def download_file_with_resume(self, message, output_file: str):
        file_size = message.document.size
        
        # Check if partial file exists
        if os.path.exists(output_file):
            current_size = os.path.getsize(output_file)
            if current_size == file_size:
                print(f"File already downloaded: {output_file}")
                return
            print(f"Resuming download from {current_size/1024/1024:.2f}MB")
        else:
            current_size = 0
        
        # Use an internal flag to know when we've completed the download
        download_completed = False
        retries = 0
        
        while not download_completed and retries < MAX_RETRIES:
            try:
                # Get current file size before starting this attempt
                if os.path.exists(output_file):
                    current_size = os.path.getsize(output_file)
                else:
                    current_size = 0
                
                # If we've already downloaded the complete file, we're done
                if current_size >= file_size:
                    download_completed = True
                    break
                
                # Create a progress bar for this attempt
                with tqdm(total=file_size, initial=current_size, desc="Downloading", 
                         unit='B', unit_scale=True) as progress_bar:
                    
                    # Define a custom progress callback for this attempt
                    last_downloaded = current_size
                    
                    async def progress_callback(downloaded_bytes, total):
                        nonlocal last_downloaded
                        
                        # Calculate the real progress
                        real_progress = current_size + downloaded_bytes
                        increment = real_progress - last_downloaded
                        
                        if increment > 0:
                            progress_bar.update(increment)
                            last_downloaded = real_progress
                            
                            # Check if we've reached or exceeded the target
                            if real_progress >= file_size:
                                return False  # Signal to stop the download
                    
                    # For resuming, we need to use the download_media's file parameter with 'ab' mode
                    # and handle the download in smaller chunks
                    with open(output_file, 'ab') as f:
                        result = await self.client.download_media(
                            message,
                            file=f,
                            progress_callback=progress_callback
                        )
                        
                        # If download was successful
                        if result:
                            final_size = os.path.getsize(output_file)
                            if final_size >= file_size:
                                download_completed = True
                
            except Exception as e:
                retries += 1
                print(f"\nDownload interrupted: {e}")
                if retries < MAX_RETRIES:
                    print(f"Retrying in {RETRY_DELAY} seconds... (Attempt {retries}/{MAX_RETRIES})")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    print(f"Failed after {MAX_RETRIES} attempts")
        
        # Final verification and cleanup
        if os.path.exists(output_file):
            final_size = os.path.getsize(output_file)
            
            # If file is larger than expected, truncate it
            if final_size > file_size:
                print(f"\nWarning: Downloaded file is larger ({final_size} bytes) than expected ({file_size} bytes). Trimming...")
                with open(output_file, 'rb+') as f:
                    f.truncate(file_size)
                final_size = file_size
                
            if final_size == file_size:
                print(f"\nDownload complete: {output_file}")
            else:
                print(f"\nWarning: File size mismatch. Expected: {file_size}, Got: {final_size}")
        else:
            print("\nError: Download failed completely")

async def download_last_saved_message():
    async with TelegramClient('session_name', api_id, api_hash) as client:
        await client.connect()
        
        if not await client.is_user_authorized():
            print("You need to authenticate first. Please run the auth script.")
            return
            
        try:
            saved_messages = await client.get_messages("me", limit=1)
            
            if not saved_messages:
                print("No messages found in Saved Messages.")
                return
                
            last_message = saved_messages[0]
            if not (last_message.media and last_message.document):
                print("The last message does not contain a document or media.")
                return
                
            file_name = getattr(last_message.document.attributes[0], 'file_name', 
                              f"file_{last_message.id}")
            # Use downloads folder for output file
            output_file = os.path.join(DOWNLOADS_FOLDER, file_name)
            
            download_manager = DownloadManager(client)
            await download_manager.download_file_with_resume(last_message, output_file)
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(download_last_saved_message())