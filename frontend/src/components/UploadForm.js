import React, { useState, useRef, useEffect } from "react";
import "./UploadForm.css";

// Component for displaying styled error messages
const ErrorDisplay = ({ processResult }) => {
  return (
    processResult &&
    processResult.error_count > 0 && (
      <div className="processing-result">
        <h3 className="result-header">Processing Report</h3>
        <p className="result-summary">
          Total records:{" "}
          <span className="result-number">{processResult.total_records}</span>
        </p>
        <p className="result-summary">
          Successfully processed:{" "}
          <span className="result-number">{processResult.processed_count}</span>
        </p>
        <div className="error-summary-section">
          <h4 className="error-header">
            Errors: {processResult.error_count} records failed
          </h4>
          <div className="error-table-container">
            <table className="error-table">
              <thead>
                <tr>
                  <th>Row Number</th>
                  <th>Error Messages</th>
                </tr>
              </thead>
              <tbody>
                {processResult.errors.map((error, index) => (
                  <tr key={index}>
                    <td>{error.row_number}</td>
                    <td>
                      <ul>
                        {error.error_messages.map((msg, subIndex) => (
                          <li key={subIndex}>{msg}</li>
                        ))}
                      </ul>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    )
  );
};

// Professional Year Picker component
const YearPicker = ({ selectedYear, setSelectedYear }) => {
  const currentYear = new Date().getFullYear();

  const handleYearChange = (e) => {
    const value = e.target.value;
    if (/^(\d{0,4})(-\d{0,4})?$/.test(value)) {
      setSelectedYear(value);
    }
  };

  const handleBlur = (e) => {
    const value = e.target.value;
    const parts = value.split("-");
    if (parts.length === 1 && /^\d{4}$/.test(parts[0])) {
      const startYear = parseInt(parts[0], 10);
      setSelectedYear(`${startYear}-${startYear + 1}`);
    }
  };

  const incrementYear = () => {
    if (!selectedYear || !selectedYear.includes("-")) {
      setSelectedYear(`${currentYear}-${currentYear + 1}`);
      return;
    }
    const startYear = parseInt(selectedYear.split("-")[0], 10);
    const newStartYear = startYear + 1;
    setSelectedYear(`${newStartYear}-${newStartYear + 1}`);
  };

  const decrementYear = () => {
    if (!selectedYear || !selectedYear.includes("-")) {
      setSelectedYear(`${currentYear - 1}-${currentYear}`);
      return;
    }
    const startYear = parseInt(selectedYear.split("-")[0], 10);
    const newStartYear = startYear - 1;
    setSelectedYear(`${newStartYear}-${newStartYear + 1}`);
  };

  return (
    <div className="year-picker-container">
      <input
        type="text"
        value={selectedYear}
        onChange={handleYearChange}
        onBlur={handleBlur}
        placeholder="YYYY-YYYY"
        className="year-picker-input"
      />
      <div className="year-picker-controls">
        <button
          type="button"
          onClick={incrementYear}
          className="year-picker-button"
        >
          &#9650;
        </button>
        <button
          type="button"
          onClick={decrementYear}
          className="year-picker-button"
        >
          &#9660;
        </button>
      </div>
    </div>
  );
};

// Confirmation Modal for duplicate filenames
const ConfirmationModal = ({ message, onConfirm, onCancel }) => {
  return (
    <div className="modal-overlay">
      <div className="confirmation-modal">
        <p>{message}</p>
        <div className="modal-actions">
          <button onClick={onConfirm} className="confirm-button">
            Continue Upload
          </button>
          <button onClick={onCancel} className="cancel-button">
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
};

const UploadForm = () => {
  const [file, setFile] = useState(null);
  const [fileType, setFileType] = useState("");
  const [institution_code, setInstitutionCode] = useState("");
  const [institutes, setInstitutes] = useState([]);
  const [academicYear, setAcademicYear] = useState("");
  const [academicQuarter, setAcademicQuarter] = useState("");
  const [message, setMessage] = useState("");
  const [showPreview, setShowPreview] = useState(false);
  const [previewData, setPreviewData] = useState([]);
  const [previewHeaders, setPreviewHeaders] = useState([]);
  const [showFilenameConfirmModal, setShowFilenameConfirmModal] =
    useState(false);

  const [isUploading, setIsUploading] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);
  const [processResult, setProcessResult] = useState(null);

  const fileInputRef = useRef(null);
  const academicQuarters = ["Jan-Mar", "Apr-Jun", "Jul-Sep", "Oct-Dec"];

  useEffect(() => {
    const fetchInstitutes = async () => {
      try {
        const response = await fetch("http://localhost:5000/institutes");
        if (response.ok) {
          const data = await response.json();
          setInstitutes(data);
        } else {
          setMessage("Error: Could not fetch the list of institutes.");
        }
      } catch (error) {
        setMessage(`Network error while fetching institutes: ${error.message}`);
      }
    };
    fetchInstitutes();
  }, []);

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      setFile(selectedFile);
      setProcessResult(null);
    }
  };

  const handleDragOver = (e) => e.preventDefault();
  const handleDrop = (e) => {
    e.preventDefault();
    const droppedFile = e.dataTransfer.files[0];
    if (droppedFile) {
      setFile(droppedFile);
      setProcessResult(null);
    }
  };

  const handleDownloadSample = async () => {
    if (!fileType || !institution_code) {
      setMessage(
        "Please select a File Type and Institute to download the sample."
      );
      return;
    }
    setMessage("Preparing sample file for download...");
    try {
      const response = await fetch(
        `http://localhost:5000/download_sample?fileType=${fileType}&institution_code=${institution_code}`
      );
      if (response.ok) {
        const contentDisposition = response.headers.get("Content-Disposition");
        let filename = `sample_${fileType}_template.xlsx`;
        if (contentDisposition) {
          const filenameMatch = contentDisposition.match(/filename="(.+)"/);
          if (filenameMatch && filenameMatch.length === 2) {
            filename = filenameMatch[1];
          }
        }
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        window.URL.revokeObjectURL(url);
        setMessage("Sample file downloaded successfully.");
      } else {
        const errorData = await response.json();
        setMessage(`Error downloading file: ${errorData.error}`);
      }
    } catch (error) {
      setMessage(`Network error: ${error.message}`);
    }
  };

  const handlePreview = async (e) => {
    e.preventDefault();
    if (!file || !fileType || !institution_code) {
      setMessage("Please fill all the required fields and select a file.");
      return;
    }
    if (fileType === "Student Details" && !academicYear) {
      setMessage("Please select an Academic Year for Student Details.");
      return;
    }
    if (
      fileType === "Fees Summary Report" &&
      (!academicYear || !academicQuarter)
    ) {
      setMessage(
        "Please select Academic Year and Quarter for Fees Summary Report."
      );
      return;
    }

    setMessage("Generating preview...");
    const formData = new FormData();
    formData.append("file", file);
    formData.append("tableType", fileType);
    formData.append("institution_code", institution_code);

    if (fileType === "Student Details" || fileType === "Fees Summary Report") {
      formData.append("academicYear", academicYear);
    }
    if (fileType === "Fees Summary Report") {
      formData.append("academicQuarter", academicQuarter);
    }

    try {
      const response = await fetch("http://localhost:5000/preview", {
        method: "POST",
        body: formData,
      });
      const data = await response.json();
      if (response.ok) {
        setPreviewHeaders(data.headers);
        setPreviewData(data.preview_data);
        setShowPreview(true);
        setMessage("");
      } else {
        setMessage(`Error: ${data.error}`);
      }
    } catch (error) {
      setMessage(`Network error: ${error.message}`);
    }
  };

  const proceedWithUpload = async () => {
    setIsUploading(true);
    setShowPreview(false);
    setMessage("Uploading file to staging table...");
    setProcessResult(null);

    const formData = new FormData();
    formData.append("file", file);
    formData.append("tableType", fileType);
    formData.append("institution_code", institution_code);

    if (fileType === "Student Details" || fileType === "Fees Summary Report") {
      formData.append("academicYear", academicYear);
    }
    if (fileType === "Fees Summary Report") {
      formData.append("academicQuarter", academicQuarter);
    }

    try {
      const uploadResponse = await fetch("http://localhost:5000/upload", {
        method: "POST",
        body: formData,
      });
      const uploadData = await uploadResponse.json();
      if (!uploadResponse.ok) {
        setMessage(`Error during upload: ${uploadData.error}`);
        setIsUploading(false);
        return;
      }
      setMessage("Upload successful. Starting data processing...");
      setIsUploading(false);
      setIsProcessing(true);

      const processResponse = await fetch(
        "http://localhost:5000/process_upload",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            uploaded_file_id: uploadData.uploaded_file_id,
            table_type: fileType,
            institution_code: institution_code,
          }),
        }
      );
      const processData = await processResponse.json();
      if (!processResponse.ok) {
        setMessage(`Error during processing: ${processData.error}`);
        setIsProcessing(false);
        return;
      }
      setProcessResult(processData);
      setMessage(processData.message);
      setFile(null);
      setFileType("");
      setInstitutionCode("");
      setAcademicYear("");
      setAcademicQuarter("");
      if (fileInputRef.current) fileInputRef.current.value = "";
      setPreviewData([]);
      setPreviewHeaders([]);
    } catch (error) {
      setMessage(`Network error: ${error.message}`);
    } finally {
      setIsUploading(false);
      setIsProcessing(false);
    }
  };

  const handleConfirmAndProcess = async () => {
    setShowPreview(false);
    try {
      const response = await fetch("http://localhost:5000/check_filename", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ filename: file.name }),
      });
      const data = await response.json();
      if (data.exists) {
        setShowFilenameConfirmModal(true);
      } else {
        proceedWithUpload();
      }
    } catch (error) {
      setMessage(`Network error during filename check: ${error.message}`);
    }
  };

  const handleCancelUpload = () => {
    setShowPreview(false);
    setMessage("Upload cancelled.");
  };

  return (
    <div className="upload-container">
      <h1>Upload Module</h1>
      <form onSubmit={handlePreview}>
        <div
          className="file-drop-zone"
          onDragOver={handleDragOver}
          onDrop={handleDrop}
          onClick={() => fileInputRef.current.click()}
        >
          {file
            ? `Selected file: ${file.name}`
            : "Drag & drop your Excel file here or click to browse"}
          <input
            type="file"
            accept=".xlsx, .xls, .csv"
            onChange={handleFileChange}
            ref={fileInputRef}
            style={{ display: "none" }}
          />
        </div>
        <br />
        <div className="selectors">
          <div className="form-group">
            <label htmlFor="fileType">File Type:</label>
            <select
              id="fileType"
              value={fileType}
              onChange={(e) => setFileType(e.target.value)}
              required
            >
              <option value="">Select File Type</option>
              <option value="Student Details">Student Details</option>
              <option value="Fees Summary Report">Fees Summary Report</option>
            </select>
          </div>
          <div className="form-group">
            <label htmlFor="institute">Institute:</label>
            <select
              id="institute"
              value={institution_code}
              onChange={(e) => setInstitutionCode(e.target.value)}
              required
            >
              <option value="">Select Institute</option>
              {institutes.map((inst) => (
                <option
                  key={inst.institution_code}
                  value={inst.institution_code}
                >
                  {inst.institute_name}
                </option>
              ))}
            </select>
          </div>
          {(fileType === "Student Details" ||
            fileType === "Fees Summary Report") && (
            <div className="form-group">
              <label htmlFor="academicYear">Academic Year:</label>
              <YearPicker
                selectedYear={academicYear}
                setSelectedYear={setAcademicYear}
              />
            </div>
          )}
          {fileType === "Fees Summary Report" && (
            <div className="form-group">
              <label htmlFor="academicQuarter">Academic Quarter:</label>
              <select
                id="academicQuarter"
                value={academicQuarter}
                onChange={(e) => setAcademicQuarter(e.target.value)}
                required
              >
                <option value="">Select Quarter</option>
                {academicQuarters.map((quarter) => (
                  <option key={quarter} value={quarter}>
                    {quarter}
                  </option>
                ))}
              </select>
            </div>
          )}
        </div>
        <div className="button-group">
          <button
            type="button"
            onClick={handleDownloadSample}
            disabled={
              !fileType || !institution_code || isUploading || isProcessing
            }
            className="download-button"
          >
            Download Sample Sheet
          </button>
          <button
            type="submit"
            disabled={
              !file ||
              !fileType ||
              !institution_code ||
              (fileType === "Student Details" && !academicYear) ||
              (fileType === "Fees Summary Report" &&
                (!academicYear || !academicQuarter)) ||
              isUploading ||
              isProcessing
            }
            className="upload-button"
          >
            {isUploading || isProcessing
              ? "Processing..."
              : "Preview and Upload"}
          </button>
        </div>
      </form>

      {(message || isUploading || isProcessing) && (
        <div className="message">
          {isUploading && "Uploading file..."}
          {isProcessing && "Processing data..."}
          {message && !isUploading && !isProcessing && message}
        </div>
      )}

      {showPreview && (
        <div className="modal-overlay">
          <div className="modal">
            <h2>Preview Data</h2>
            <div className="table-container">
              <table>
                <thead>
                  <tr>
                    {previewHeaders.map((header, index) => (
                      <th key={index}>{header}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {previewData.map((row, rowIndex) => (
                    <tr key={rowIndex}>
                      {row.map((cell, cellIndex) => (
                        <td key={cellIndex}>{cell}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="modal-actions">
              <button
                onClick={handleConfirmAndProcess}
                disabled={isUploading || isProcessing}
              >
                {isUploading
                  ? "Uploading..."
                  : isProcessing
                  ? "Processing..."
                  : "Confirm Upload"}
              </button>
              <button onClick={handleCancelUpload} className="cancel-button">
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {showFilenameConfirmModal && (
        <ConfirmationModal
          message="A file with this name has been uploaded before. Do you want to continue?"
          onConfirm={() => {
            setShowFilenameConfirmModal(false);
            proceedWithUpload();
          }}
          onCancel={() => setShowFilenameConfirmModal(false)}
        />
      )}

      <ErrorDisplay processResult={processResult} />
    </div>
  );
};

export default UploadForm;
