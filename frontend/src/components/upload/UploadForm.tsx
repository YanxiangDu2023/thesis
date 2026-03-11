import { useState } from "react";

type UploadFormProps = {
  label: string;
  title: string;
};

function UploadForm({ label, title }: UploadFormProps) {
  const [file, setFile] = useState<File | null>(null);
  const [message, setMessage] = useState("");
  const [status, setStatus] = useState<"idle" | "uploading" | "success" | "error">("idle");

  const handleUpload = async () => {
    if (!file) {
      alert("Please select a CSV file first.");
      return;
    }

    const formData = new FormData();
    formData.append("matrix_type", label);
    formData.append("file", file);

    try {
      setStatus("uploading");
      setMessage("Uploading...");

      const response = await fetch("http://127.0.0.1:8001/uploads/csv", {
        method: "POST",
        body: formData,
      });

      const result = await response.json();

      if (!response.ok) {
        throw new Error(result.detail || "Upload failed");
      }

      setStatus("success");
      setMessage(`Upload successful. Upload ID: ${result.upload_run_id}`);

      console.log(result);
    } catch (error) {
      console.error(error);
      setStatus("error");
      setMessage("Upload failed.");
    }
  };

  return (
    <div className="upload-card">
      <h3>{title}</h3>

      <input
        type="file"
        accept=".csv"
        onChange={(e) => {
          const selectedFile = e.target.files?.[0] || null;
          setFile(selectedFile);
          setMessage("");
          setStatus("idle");
        }}
      />

      {file && <p>Selected file: {file.name}</p>}

      <button type="button" onClick={handleUpload}>
        Upload
      </button>

      {status === "uploading" && <p style={{ color: "blue" }}>Uploading...</p>}
      {status === "success" && <p style={{ color: "green" }}>✅ {message}</p>}
      {status === "error" && <p style={{ color: "red" }}>❌ {message}</p>}
    </div>
  );
}

export default UploadForm;