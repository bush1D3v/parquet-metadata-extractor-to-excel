import React, { useState } from 'react';
import './App.css';

function App() {
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [downloadUrl, setDownloadUrl] = useState(null);

  const handleFileChange = (e) => {
    const selectedFiles = Array.from(e.target.files);
    const parquetFiles = selectedFiles.filter(file => file.name.endsWith('.parquet'));
    
    if (parquetFiles.length === 0) {
      setError('Por favor, selecione apenas arquivos Parquet (.parquet)');
      return;
    }
    
    setFiles(parquetFiles);
    setError(null);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (files.length === 0) {
      setError('Por favor, selecione pelo menos um arquivo Parquet');
      return;
    }
    
    setLoading(true);
    setError(null);
    
    const formData = new FormData();
    files.forEach(file => {
      formData.append('files', file);
    });
    
    try {
      const response = await fetch('http://localhost:5000/upload', {
        method: 'POST',
        body: formData,
      });
      
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.error || 'Erro ao processar arquivos');
      }
      
      setDownloadUrl('http://localhost:5000' + data.excel_url);
      
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <header className="header">
        <h1>Extrator de Metadados Parquet</h1>
      </header>
      
      <main className="main-content">
        <form onSubmit={handleSubmit} className="upload-form">
          <div className="file-input-container">
            <label htmlFor="file-upload" className="file-input-label">
              Selecionar Arquivos Parquet
            </label>
            <input
              type="file"
              id="file-upload"
              multiple
              onChange={handleFileChange}
              accept=".parquet"
              className="file-input"
            />
          </div>
          
          {files.length > 0 && (
            <div className="selected-files">
              <h3>Arquivos Selecionados:</h3>
              <ul>
                {files.map((file, index) => (
                  <li key={index}>{file.name}</li>
                ))}
              </ul>
            </div>
          )}
          
          {error && <div className="error-message">{error}</div>}
          
          <button 
            type="submit" 
            className="submit-button" 
            disabled={loading || files.length === 0}
          >
            {loading ? 'Processando...' : 'Gerar Excel'}
          </button>
        </form>
        
        {downloadUrl && (
          <div className="download-section">
            <h3>Excel Gerado com Sucesso!</h3>
            <a 
              href={downloadUrl} 
              className="download-button"
              target="_blank" 
              rel="noopener noreferrer"
            >
              Baixar Arquivo Excel
            </a>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;