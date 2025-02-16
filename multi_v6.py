import os
import asyncio
import io
from tqdm import tqdm
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename
from dotenv import load_dotenv

# Load .env file
load_dotenv()
api_id = os.getenv("api_id")
api_hash = os.getenv("api_hash")
phone_number = os.getenv("phone_number")

DOWNLOADS_FOLDER = "downloads"  # Define downloads folder
MAX_CONCURRENT_DOWNLOADS = 16  # Maximum number of concurrent chunks
CHUNK_SIZE = 1024 * 1024  # 1MB chunk size
MAX_RETRIES = 3
RETRY_DELAY = 3  # seconds

# Ensure downloads directory exists
os.makedirs(DOWNLOADS_FOLDER, exist_ok=True)


async def download_chunk(client, message, start, end, output_file, progress_bar, retries=0):
    try:
        # Calculate the chunk size
        chunk_size = end - start + 1
        
        # Use iter_download to download the specific chunk
        buffer = io.BytesIO()
        downloaded = 0
        
        async for chunk in client.iter_download(
            message.media,
            offset=start,
            request_size=1024 * 1024  # 1MB request size
        ):
            # Write to buffer
            buffer.write(chunk)
            downloaded += len(chunk)
            progress_bar.update(len(chunk))
            
            # Break if we've downloaded enough
            if downloaded >= chunk_size:
                break
        
        # Write the buffer to file at the correct position
        with open(output_file, "r+b") as f:
            f.seek(start)
            f.write(buffer.getvalue()[:chunk_size])  # Only write up to chunk_size
            
    except Exception as e:
        if retries < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAY)
            await download_chunk(client, message, start, end, output_file, progress_bar, retries + 1)
        else:
            print(f"Failed to download chunk {start}-{end} after {MAX_RETRIES} retries: {e}")


async def download_file_concurrently(client, message, output_file, file_size):
    # Calculate optimal chunk size and number of chunks
    connections = min(MAX_CONCURRENT_DOWNLOADS, max(1, file_size // CHUNK_SIZE))
    chunk_size = file_size // connections
    
    # Create progress bar
    progress_bar = tqdm(total=file_size, desc="Downloading", unit='B', unit_scale=True, unit_divisor=1024)
    
    # Create and gather download tasks
    tasks = []
    for i in range(connections):
        start = i * chunk_size
        
        # Make sure the last chunk gets the remainder
        if i == connections - 1:
            end = file_size - 1
        else:
            end = start + chunk_size - 1
        
        task = asyncio.create_task(
            download_chunk(client, message, start, end, output_file, progress_bar)
        )
        tasks.append(task)
    
    await asyncio.gather(*tasks)
    progress_bar.close()


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
                
            # Get filename from document attributes
            file_name = None
            for attr in last_message.document.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    file_name = attr.file_name
                    break
            
            if not file_name:
                file_name = f"file_{last_message.id}"
                
            output_file = os.path.join(DOWNLOADS_FOLDER, file_name)
            file_size = last_message.document.size
            
            # Create an empty file with the required size
            with open(output_file, "wb") as f:
                f.truncate(file_size)
            
            # Download the file concurrently
            print(f"Downloading file: {file_name} (Size: {file_size / 1024 / 1024:.2f} MB)")
            await download_file_concurrently(client, last_message, output_file, file_size)
            print(f"Download complete: {output_file}")
                
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(download_last_saved_message())