'use client';

import { useState, useEffect, ChangeEvent, FormEvent } from 'react';
import Link from 'next/link';

interface FileRecord {
  id: number;
  filename: string;
  total_chunks: number;
}

const backendUrl = 'https://miniproject6thsem-production.up.railway.app';
const CHUNK_SIZE = 1024 * 1024; // 1 MB

// Helper to concatenate an array of Uint8Array chunks into one ArrayBuffer
function concatChunks(chunks: Uint8Array[]): ArrayBuffer {
  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result.buffer;
}

// Download a single DFS chunk with progress tracking.
async function downloadChunk(
  file_id: number,
  chunk_order: number,
  onProgress: (progress: number) => void
): Promise<ArrayBuffer> {
  const response = await fetch(
    `${backendUrl}/download_chunk?file_id=${file_id}&chunk_order=${chunk_order}`
  );
  if (!response.body) throw new Error('No response body');

  const total = Number(response.headers.get('Content-Length')) || CHUNK_SIZE;
  const reader = response.body.getReader();
  let received = 0;
  const chunks: Uint8Array[] = [];

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) {
      chunks.push(value);
      received += value.length;
      onProgress(Math.min(100, (received / total) * 100));
    }
  }
  return concatChunks(chunks);
}

export default function DFSPage() {
  const [fileList, setFileList] = useState<FileRecord[]>([]);
  const [file, setFile] = useState<File | null>(null);
  const [downloadProgress, setDownloadProgress] = useState<number[]>([]);
  const [downloadingFile, setDownloadingFile] = useState<FileRecord | null>(null);
  const [downloadTime, setDownloadTime] = useState<number | null>(null);

  // Fetch DFS file list
  const fetchFiles = async () => {
    const res = await fetch(`${backendUrl}/list`);
    const data = await res.json();
    setFileList(data.files);
  };

  useEffect(() => {
    fetchFiles();
  }, []);

  const handleFileChange = (e: ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) setFile(e.target.files[0]);
  };

  const handleUploadSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!file) return;
    const formData = new FormData();
    formData.append('file', file);
    const res = await fetch(`${backendUrl}/upload`, {
      method: 'POST',
      body: formData,
    });
    if (res.ok) {
      setFile(null);
      fetchFiles();
    }
  };

  // Download DFS file via parallel chunk downloads
  const handleDownload = async (fileRecord: FileRecord) => {
    setDownloadingFile(fileRecord);
    const totalChunks = fileRecord.total_chunks;
    setDownloadProgress(new Array(totalChunks).fill(0));
    setDownloadTime(null);

    const startTime = Date.now();
    const promises = [];
    for (let i = 1; i <= totalChunks; i++) {
      promises.push(
        downloadChunk(fileRecord.id, i, (prog) => {
          setDownloadProgress((prev) => {
            const newProg = [...prev];
            newProg[i - 1] = prog;
            return newProg;
          });
        })
      );
    }
    const chunksData = await Promise.all(promises);
    const endTime = Date.now();
    setDownloadTime(endTime - startTime);

    const mergedBuffer = concatChunks(
      chunksData.map((chunk) => new Uint8Array(chunk))
    );
    const blob = new Blob([mergedBuffer]);
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = fileRecord.filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    // Clear in-progress states (but keep downloadTime to show the final result)
    setDownloadingFile(null);
    setDownloadProgress([]);
  };

  return (
    <div className="container mx-auto p-4">
      <h1 className="text-2xl font-bold mb-4">DFS Pipeline (Chunked Files)</h1>
      <Link href="/nodfs" className="text-blue-500 underline">
        Switch to No-DFS Pipeline
      </Link>

      {/* Upload Form */}
      <form onSubmit={handleUploadSubmit} className="mb-4">
        <input type="file" onChange={handleFileChange} className="border p-2" />
        <button type="submit" className="bg-blue-500 text-white p-2 rounded ml-2">
          Upload
        </button>
      </form>

      {/* File List */}
      <h2 className="text-xl font-semibold mb-2">Uploaded Files</h2>
      <ul>
        {fileList.map((fileItem) => (
          <li key={fileItem.id} className="border p-2 mb-1 flex justify-between items-center">
            <span>{fileItem.filename}</span>
            <button
              className="bg-green-500 text-white p-2 rounded"
              onClick={() => handleDownload(fileItem)}
            >
              Download
            </button>
          </li>
        ))}
      </ul>

      {/* Download Progress */}
      {downloadingFile && downloadProgress.length > 0 && (
        <div className="mt-4">
          <h3 className="text-lg font-semibold">
            Downloading: {downloadingFile.filename}
          </h3>
          <div className="grid grid-cols-3 gap-4">
            {downloadProgress.map((prog, idx) => (
              <div key={idx} className="border p-2">
                <p>Chunk {idx + 1}</p>
                <div className="w-full bg-gray-300 h-4 rounded">
                  <div className="bg-blue-500 h-4 rounded" style={{ width: `${prog}%` }}></div>
                </div>
                <p>{prog.toFixed(0)}%</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Download Completion Time */}
      {downloadTime !== null && (
        <p className="mt-4 text-lg">
          Download completed in {downloadTime} ms.
        </p>
      )}
    </div>
  );
}
