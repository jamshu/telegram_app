from telethon import TelegramClient
import os
import asyncio
from dotenv import load_dotenv
from tqdm import tqdm
import math
from dataclasses import dataclass
from typing import List, Set, Dict
import aiofiles
import logging
import json
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.network.connection import ConnectionTcpFull
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputDocumentFileLocation
from telethon.sessions import MemorySession

# Load .env file
load_dotenv()
api_id = os.getenv("api_id")
api_hash = os.getenv("api_hash")

# Constants
DOWNLOADS_FOLDER = "downloads"
TEMP_FOLDER = "downloads/temp"
CHUNK_SIZE = 1024 * 1024  # 1MB
MAX_WORKERS = 8
RETRIES = 5
RETRY_DELAY = 1

@dataclass
class DownloadChunk:
    start: int
    end: int
    offset: int
    size: int
    temp_file: str
    downloaded: bool = False

class ParallelDownloadManager:
    def __init__(self, client: TelegramClient):
        self.client = client
        self.dc_id = None
        self.workers: List[MTProtoSender] = []
        self.session = MemorySession()
        os.makedirs(DOWNLOADS_FOLDER, exist_ok=True)
        os.makedirs(TEMP_FOLDER, exist_ok=True)

    async def _create_workers(self):
        """Create multiple MTProto senders for parallel downloads"""
        if not self.workers:
            main_sender = self.client._sender
            for _ in range(MAX_WORKERS):
                sender = await self._create_sender()
                self.workers.append(sender)

    async def _create_sender(self):
        """Create a new sender with proper connection"""
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(
            self.session,
            loggers=self.client._log  # Pass the loggers from the client
        )
        # Create a connection object with dc_id and loggers
        connection = ConnectionTcpFull(
            ip=dc.ip_address,
            port=dc.port,
            dc_id=dc.id,
            loggers=self.client._log  # Pass the loggers from the client
        )
        await sender.connect(connection)

        # Copy the auth key from the main client's sender
        if hasattr(self.client._sender, '_auth_key'):
            sender._auth_key = self.client._sender._auth_key
            # Ensure the session has the auth key
            self.session.set_auth_key(sender._auth_key)
        return sender

    async def _download_chunk(self, chunk: DownloadChunk, 
                            input_location: InputDocumentFileLocation,
                            worker: MTProtoSender) -> bool:
        """Download a single chunk using a worker"""
        if chunk.downloaded and os.path.exists(chunk.temp_file):
            if os.path.getsize(chunk.temp_file) == chunk.size:
                return True

        for retry in range(RETRIES):
            try:
                request = GetFileRequest(
                    location=input_location,
                    offset=chunk.offset,
                    limit=chunk.size
                )
                # Use the send method to send the request
                result = await worker.send(request)

                async with aiofiles.open(chunk.temp_file, 'wb') as f:
                    await f.write(result.bytes)

                chunk.downloaded = True
                return True

            except Exception as e:
                if retry == RETRIES - 1:
                    logging.error(f"Failed to download chunk at offset {chunk.offset}: {e}")
                    return False
                await asyncio.sleep(RETRY_DELAY)
        return False

    # Rest of the class remains unchanged...

    async def _init_download(self, message) -> InputDocumentFileLocation:
        """Initialize download and get file location"""
        document = message.document
        self.dc_id = document.dc_id
        input_location = InputDocumentFileLocation(
            id=document.id,
            access_hash=document.access_hash,
            file_reference=document.file_reference,
            thumb_size=''
        )
        await self._create_workers()
        return input_location

    def _get_state_file(self, file_id: int) -> str:
        """Get path to download state file"""
        return os.path.join(TEMP_FOLDER, f"{file_id}_state.json")
        
    def _get_chunk_file(self, file_id: int, chunk_num: int) -> str:
        """Get path to chunk temporary file"""
        return os.path.join(TEMP_FOLDER, f"{file_id}_chunk_{chunk_num}")

    async def _save_download_state(self, file_id: int, chunks: List[DownloadChunk]):
        """Save download progress to state file"""
        state = {
            'file_id': file_id,
            'chunks': [
                {
                    'offset': chunk.offset,
                    'size': chunk.size,
                    'downloaded': chunk.downloaded,
                    'temp_file': chunk.temp_file
                }
                for chunk in chunks
            ]
        }
        async with aiofiles.open(self._get_state_file(file_id), 'w') as f:
            await f.write(json.dumps(state))
            
    async def _load_download_state(self, file_id: int) -> List[DownloadChunk]:
        """Load previous download state if exists"""
        state_file = self._get_state_file(file_id)
        if not os.path.exists(state_file):
            return None
            
        async with aiofiles.open(state_file, 'r') as f:
            state = json.loads(await f.read())
            chunks = []
            for chunk_data in state['chunks']:
                chunk = DownloadChunk(
                    start=chunk_data['offset'],
                    end=chunk_data['offset'] + chunk_data['size'],
                    offset=chunk_data['offset'],
                    size=chunk_data['size'],
                    temp_file=chunk_data['temp_file'],
                    downloaded=chunk_data['downloaded']
                )
                if chunk.downloaded and os.path.exists(chunk.temp_file):
                    if os.path.getsize(chunk.temp_file) == chunk.size:
                        chunks.append(chunk)
                        continue
                chunk.downloaded = False
                chunks.append(chunk)
            return chunks

    def _calculate_chunks(self, file_id: int, file_size: int) -> List[DownloadChunk]:
        """Split file into chunks for parallel download"""
        chunks = []
        chunk_size = CHUNK_SIZE
        offset = 0
        chunk_num = 0
        
        while offset < file_size:
            chunk_end = min(offset + chunk_size, file_size)
            chunks.append(DownloadChunk(
                start=offset,
                end=chunk_end,
                offset=offset,
                size=chunk_end - offset,
                temp_file=self._get_chunk_file(file_id, chunk_num),
                downloaded=False
            ))
            offset = chunk_end
            chunk_num += 1
            
        return chunks

    
    async def _merge_chunks(self, chunks: List[DownloadChunk], output_file: str):
        """Merge downloaded chunks into final file"""
        async with aiofiles.open(output_file, 'wb') as f:
            for chunk in sorted(chunks, key=lambda x: x.offset):
                if not (chunk.downloaded and os.path.exists(chunk.temp_file)):
                    raise Exception(f"Missing chunk at offset {chunk.offset}")
                    
                async with aiofiles.open(chunk.temp_file, 'rb') as chunk_file:
                    await f.write(await chunk_file.read())

    def _cleanup_temp_files(self, file_id: int):
        """Remove temporary files after successful download"""
        try:
            state_file = self._get_state_file(file_id)
            if os.path.exists(state_file):
                os.remove(state_file)
                
            for file in os.listdir(TEMP_FOLDER):
                if file.startswith(f"{file_id}_chunk_"):
                    os.remove(os.path.join(TEMP_FOLDER, file))
        except Exception as e:
            logging.error(f"Error cleaning up temp files: {e}")

    async def download_file(self, message, output_file: str):
        """Download file in parallel chunks with resume capability"""
        try:
            file_size = message.document.size
            file_id = message.document.id
            output_path = os.path.join(DOWNLOADS_FOLDER, output_file)
            
            if os.path.exists(output_path):
                if os.path.getsize(output_path) == file_size:
                    print(f"File already downloaded: {output_path}")
                    return
                    
            print(f"Starting parallel download of {file_size/1024/1024:.2f}MB file")
            
            input_location = await self._init_download(message)
            chunks = await self._load_download_state(file_id)
            if not chunks:
                chunks = self._calculate_chunks(file_id, file_size)
            else:
                print("Resuming previous download...")
                
            downloaded_size = sum(chunk.size for chunk in chunks if chunk.downloaded)
            with tqdm(total=file_size, initial=downloaded_size, desc="Downloading", 
                     unit='B', unit_scale=True) as progress:
                
                while True:
                    remaining_chunks = [c for c in chunks if not c.downloaded]
                    if not remaining_chunks:
                        break
                        
                    tasks = []
                    for chunk, worker in zip(remaining_chunks, self.workers):
                        task = asyncio.create_task(
                            self._download_chunk(chunk, input_location, worker)
                        )
                        tasks.append(task)
                        
                    results = await asyncio.gather(*tasks)
                    
                    downloaded_size = sum(chunk.size for chunk in chunks if chunk.downloaded)
                    progress.n = downloaded_size
                    progress.refresh()
                    await self._save_download_state(file_id, chunks)
                    
            print("Merging chunks...")
            await self._merge_chunks(chunks, output_path)
            
            if os.path.exists(output_path):
                if os.path.getsize(output_path) == file_size:
                    print(f"Download complete: {output_path}")
                    self._cleanup_temp_files(file_id)
                else:
                    print("Error: File size mismatch")
            
        finally:
            for worker in self.workers:
                await worker.disconnect()
            self.workers = []

async def download_last_saved_message():
    async with TelegramClient('session_name', api_id, api_hash) as client:
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
                
            downloader = ParallelDownloadManager(client)
            await downloader.download_file(last_message, file_name)
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(download_last_saved_message())