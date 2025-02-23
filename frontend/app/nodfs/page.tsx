'use client';

import { useState, useEffect, ChangeEvent, FormEvent } from 'react';
import Link from 'next/link';

interface NoDFSFile {
  id: number;
  filename: string;
  file_size: number;
}

const backendUrl = 'https://miniproject6thsem-production-a78e.up.railway.app';

export default function NoDFSPage() {
  const [fileList, setFileList] = useState<NoDFSFile[]>([]);
  const [file, setFile] = useState<File | null>(null);
  const [downloadTime, setDownloadTime] = useState<number | null>(null);

  // Fetch the no-DFS file list
  const fetchFiles = async () => {
    const res = await fetch(`${backendUrl}/list_nodfs`);
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
    const res = await fetch(`${backendUrl}/upload_nodfs`, {
      method: 'POST',
      body: formData,
    });
    if (res.ok) {
      setFile(null);
      fetchFiles();
    }
  };

  // Download the file as a whole and measure download time
  const handleDownload = async (fileItem: NoDFSFile) => {
    const startTime = Date.now();
    const response = await fetch(`${backendUrl}/download_nodfs?file_id=${fileItem.id}`);
    const blob = await response.blob();
    const endTime = Date.now();
    setDownloadTime(endTime - startTime);
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = fileItem.filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
  };

  return (
    <div className="container mx-auto p-4">
      <h1 className="text-2xl font-bold mb-4">No-DFS Pipeline (Whole Files)</h1>
      <Link href="/" className="text-blue-500 underline">
        Switch to DFS Pipeline
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

      {/* Download Time */}
      {downloadTime !== null && (
        <div className="mt-4">
          <p className="text-lg">
            Download completed in {downloadTime} ms.
          </p>
        </div>
      )}
    </div>
  );
}
