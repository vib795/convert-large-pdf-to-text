import os
import time
from queue import Queue
import threading
from PyPDF2 import PdfReader, PdfWriter
from pdfminer.high_level import extract_text
import tempfile
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_flag = threading.Event()

def signal_handler(signum, frame):
    logger.info("Interrupt received, shutting down...")
    shutdown_flag.set()

signal.signal(signal.SIGINT, signal_handler)

def split_pdf(input_path, chunk_size=5):
    reader = PdfReader(input_path)
    total_pages = len(reader.pages)
    logger.info(f"Splitting PDF with {total_pages} pages into chunks of {chunk_size} pages")
    
    for i in range(0, total_pages, chunk_size):
        if shutdown_flag.is_set():
            break
        writer = PdfWriter()
        for j in range(i, min(i + chunk_size, total_pages)):
            writer.add_page(reader.pages[j])
        
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            writer.write(temp_file)
            temp_file_path = temp_file.name
        
        logger.debug(f"Created temporary chunk file: {temp_file_path}")
        yield temp_file_path

text_chunks = []

def process_chunk(chunk_path):
    if shutdown_flag.is_set():
        return None
    start_time = time.time()
    try:
        text = extract_text(chunk_path)
        processing_time = time.time() - start_time
        logger.info(f"Processed chunk: {chunk_path} in {processing_time:.2f} seconds")
        return text
    except Exception as e:
        logger.error(f"Error processing chunk {chunk_path}: {str(e)}")
        return None
    finally:
        try:
            os.unlink(chunk_path)  # Delete the temporary file
            logger.debug(f"Deleted temporary file: {chunk_path}")
        except Exception as e:
            logger.warning(f"Failed to delete temporary file {chunk_path}: {str(e)}")

def process_large_pdf(input_path, num_workers=2):
    logger.info(f"Starting to process large PDF: {input_path} with {num_workers} workers")
    start_time = time.time()

    chunk_paths = list(split_pdf(input_path))
    logger.info(f"Created {len(chunk_paths)} chunks")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_chunk = {executor.submit(process_chunk, chunk_path): chunk_path for chunk_path in chunk_paths}
        
        for future in as_completed(future_to_chunk):
            chunk_path = future_to_chunk[future]
            try:
                chunk_text = future.result()
                if chunk_text is not None:
                    text_chunks.append(chunk_text)
            except Exception as e:
                logger.error(f"Exception occurred while processing {chunk_path}: {str(e)}")

            if shutdown_flag.is_set():
                logger.info("Shutdown requested, cancelling remaining tasks...")
                executor.shutdown(wait=False)
                break

    # Combine all text chunks
    full_text = "".join(text_chunks)
    
    total_time = time.time() - start_time
    logger.info(f"PDF processing completed in {total_time:.2f} seconds")
    
    return full_text

# Usage
if __name__ == "__main__":
    input_pdf_path = "large_file2.pdf"  # Replace with your PDF file path
    output_text_path = "output2.txt"  # Path to save the extracted text
    
    try:
        extracted_text = process_large_pdf(input_pdf_path)
        
        if not shutdown_flag.is_set():
            # Save the extracted text to a file
            with open(output_text_path, 'w', encoding='utf-8') as f:
                f.write(extracted_text)
            
            logger.info(f"Text extraction complete. Output saved to {output_text_path}")
        else:
            logger.info("Process was interrupted. Partial results may have been processed.")
    except Exception as e:
        logger.error(f"An error occurred during PDF processing: {str(e)}")
    finally:
        # Clean up any remaining temporary files
        for file in os.listdir(tempfile.gettempdir()):
            if file.endswith('.pdf'):
                try:
                    os.unlink(os.path.join(tempfile.gettempdir(), file))
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {file}: {str(e)}")