import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  PieChart, Pie, Cell, LineChart, Line, AreaChart, Area
} from 'recharts';
import './App.css';

const COLORS = ['#00d4ff', '#0088fe', '#00C49F', '#FFBB28', '#FF8042'];

// Custom Tooltip for Dark Theme
const CustomTooltip = ({ active, payload, label }) => {
  if (active && payload && payload.length) {
    return (
      <div className="custom-tooltip" style={{ backgroundColor: '#020617', padding: '15px', border: '1px solid #334155', borderRadius: '10px' }}>
        <p className="label" style={{ color: '#00d4ff', fontWeight: 'bold', marginBottom: '5px' }}>{label}</p>
        <p style={{ color: '#64748b', fontSize: '11px', marginBottom: '8px' }}>🖱️ Click bar for detailed view</p>
        {payload.map((entry, index) => (
          <p key={index} style={{ color: '#fff', fontSize: '13px' }}>
            {`${entry.name}: ${Number(entry.value).toLocaleString()}`}
          </p>
        ))}
      </div>
    );
  }
  return null;
};

const ThinkingState = ({ step }) => (
  <div className="thinking-overlay">
    <div className="thinking-card">
      <div className="spinner"></div>
      <p className="thinking-text">{step}</p>
    </div>
  </div>
);

const Toast = ({ message, onClose }) => {
  useEffect(() => {
    const timer = setTimeout(onClose, 5000);
    return () => clearTimeout(timer);
  }, [onClose]);

  return (
    <div className="toast-notification">
      <div className="toast-content">
        <span className="toast-icon">⚠️</span>
        <p>{message}</p>
        <button onClick={onClose} className="toast-close">×</button>
      </div>
    </div>
  );
};

// --- Helper Components ---

const SchemaInfo = ({ mapping, onSave }) => {
  const [isEditing, setIsEditing] = useState(false);
  const [localMap, setLocalMap] = useState({});

  useEffect(() => {
    if (mapping?.current) setLocalMap(mapping.current);
  }, [mapping]);

  if (!mapping) return null;

  const handleManualSave = async () => {
    await onSave(localMap);
    setIsEditing(false);
  };

  return (
    <div className="section" style={{ marginTop: '20px', padding: '15px', background: 'rgba(0, 212, 255, 0.05)', borderRadius: '12px', border: '1px solid rgba(0, 212, 255, 0.1)' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
        <h3 style={{ fontSize: '0.7rem', letterSpacing: '1px', color: '#00d4ff', margin: 0 }}>🧬 DATASET SCHEMA</h3>
        {!isEditing ? (
          <button onClick={() => setIsEditing(true)} style={{ background: 'none', border: 'none', color: '#00d4ff', fontSize: '0.65rem', cursor: 'pointer', textDecoration: 'underline' }}>Edit</button>
        ) : (
          <div style={{ display: 'flex', gap: '8px' }}>
            <button onClick={handleManualSave} style={{ background: '#00d4ff', border: 'none', color: '#020617', fontSize: '0.6rem', padding: '2px 6px', borderRadius: '4px', cursor: 'pointer', fontWeight: 'bold' }}>Save</button>
            <button onClick={() => setIsEditing(false)} style={{ background: 'none', border: 'none', color: '#64748b', fontSize: '0.65rem', cursor: 'pointer' }}>Cancel</button>
          </div>
        )}
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
        {["Measure", "Category", "Filter"].map((role) => (
          <div key={role} style={{ display: 'flex', flexDirection: 'column', gap: '4px', fontSize: '0.75rem' }}>
            <span style={{ color: '#94a3b8', fontSize: '0.65rem', textTransform: 'uppercase' }}>{role}</span>
            {!isEditing ? (
              <span style={{ color: '#fff', fontWeight: '500', background: 'rgba(255,255,255,0.05)', padding: '4px 8px', borderRadius: '4px' }}>
                {mapping.current[role] || 'N/A'}
              </span>
            ) : (
              <select 
                value={localMap[role] || ''} 
                onChange={(e) => setLocalMap({ ...localMap, [role]: e.target.value })}
                style={{ width: '100%', background: '#020617', border: '1px solid #334155', color: '#fff', fontSize: '0.75rem', padding: '4px', borderRadius: '4px' }}
              >
                <option value="">None (Auto)</option>
                {(role === 'Measure' ? mapping.available?.numeric : mapping.available?.categorical)?.map(col => (
                  <option key={col} value={col}>{col}</option>
                ))}
              </select>
            )}
          </div>
        ))}
      </div>
      <p style={{ fontSize: '0.65rem', color: '#64748b', marginTop: '12px', lineHeight: '1.4' }}>
        {isEditing ? '⚠️ Selecting columns manually will override AI detection.' : 'ℹ️ The system mapped these roles for dynamic analysis.'}
      </p>
    </div>
  );
};

const Sidebar = ({ currentPage, setCurrentPage, onUpload, mapping, onSaveMapping }) => {
  const [dataSource, setDataSource] = useState('demo');

  return (
    <div className="sidebar">
      <div className="sidebar-header">
        <div className="logo">📁 AI DASHBOARD</div>
      </div>

      <div className="section">
        <h3>🛠️ SETTINGS</h3>
        <label style={{ display: 'block', fontSize: '0.8rem', color: '#94a3b8', marginBottom: '8px' }}>Data Source</label>
        <select value={dataSource} onChange={(e) => setDataSource(e.target.value)}>
          <option value="demo">Demo Dataset</option>
          <option value="upload">Upload Dataset</option>
        </select>

        {dataSource === 'upload' && (
          <div className="upload-box">
            <input type="file" accept=".csv" onChange={(e) => onUpload(e.target.files[0])} />
            <p className="caption" style={{ marginTop: '10px', fontSize: '0.75rem', color: '#64748b' }}>Select CSV file to replace demo data.</p>
          </div>
        )}
      </div>

      <SchemaInfo mapping={mapping} onSave={onSaveMapping} />

      <div className="section" style={{ flexGrow: 1 }}>
        <h3>🧭 NAVIGATION</h3>
        <nav>
          <button className={currentPage === 'ai-query' ? 'active' : ''} onClick={() => setCurrentPage('ai-query')}>📊 AI Query</button>
          <button className={currentPage === 'sales' ? 'active' : ''} onClick={() => setCurrentPage('sales')}>💰 Sales Analytics</button>
          <button className={currentPage === 'customer' ? 'active' : ''} onClick={() => setCurrentPage('customer')}>👤 Customer Analytics</button>
          <button className={currentPage === 'product' ? 'active' : ''} onClick={() => setCurrentPage('product')}>📦 Product Analytics</button>
        </nav>
      </div>

      <div className="sidebar-footer">
        <div style={{ padding: '1rem', background: 'rgba(255,255,255,0.03)', borderRadius: '12px', border: '1px solid rgba(255,255,255,0.05)' }}>
          <p style={{ margin: 0, fontWeight: '700', color: '#64748b' }}>API STATUS</p>
          <p style={{ margin: '5px 0 0', color: '#10b981', display: 'flex', alignItems: 'center', gap: '5px' }}>
            <span style={{ width: '8px', height: '8px', background: '#10b981', borderRadius: '50%' }}></span> Connected
          </p>
        </div>
      </div>
    </div>
  );
};

// --- Page Components ---

const AIQueryPage = ({ query, setQuery, handleSubmit, handleDrillDown, goBack, loading, result, error, drillDownPath, filters, setFilters, onExport }) => {
  const [showSql, setShowSql] = useState(false);
  const [sortOrder, setSortOrder] = useState('none');

  const getSortedData = (data) => {
    if (sortOrder === 'none' || !data || data.length === 0) return data;
    const keys = Object.keys(data[0]);
    const numKey = keys.find(k => typeof data[0][k] === 'number') || keys[0];
    return [...data].sort((a, b) => {
      const valA = a[numKey] || 0;
      const valB = b[numKey] || 0;
      return sortOrder === 'asc' ? valA - valB : valB - valA;
    });
  };

  const sortedData = getSortedData(result?.data);
  const formatLabel = (val) => val ? val.toString().replace(/_/g, ' ').toUpperCase() : 'N/A';

  const renderChart = () => {
    if (!sortedData || sortedData.length === 0) return (
      <div className="card chart-container empty">
        <h3>📊 Visualization</h3>
        <p>Run a query to see intent-based charts...</p>
      </div>
    );
    if (result.chart_type === 'table') {
      return (
        <div className="card chart-container empty success">
          <div style={{ fontSize: '3rem', marginBottom: '20px' }}>📋</div>
          <h3>Detailed Drill-down Table</h3>
          <p>Displaying unfiltered record list for the selected segment.</p>
        </div>
      );
    }
    const keys = Object.keys(sortedData[0]);
    const numericKeys = keys.filter(k => typeof sortedData[0][k] === 'number');
    const categoricalKeys = keys.filter(k => typeof sortedData[0][k] === 'string');
    const yScore = (k) => {
      const lower = k.toLowerCase();
      if (lower.includes('revenue') || lower.includes('sales') || lower.includes('amount')) return 100;
      if (lower.includes('count') || lower.includes('total') || lower.includes('rating')) return 80;
      return 10;
    };
    let yKey = [...numericKeys].sort((a, b) => yScore(b) - yScore(a))[0] || numericKeys[0];
    let xKey = categoricalKeys[0] || numericKeys.find(k => k !== yKey) || numericKeys[0];

    if (xKey && yKey) {
      const chartType = result.chart_type || 'bar';
      const dynamicWidth = Math.max(100, sortedData.length * 40);
      const isScrollable = sortedData.length > 15 && chartType !== 'pie';
      const renderVisualization = () => {
        if (chartType === 'line') {
          return (
            <AreaChart data={sortedData} margin={{ top: 20, right: 30, left: 20, bottom: 80 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
              <XAxis dataKey={xKey} stroke="#fff" tick={{fill: '#fff'}} fontSize={11} angle={-45} textAnchor="end" interval={0} />
              <YAxis stroke="#fff" tick={{fill: '#fff'}} fontSize={11} tickFormatter={(v) => Number(v).toLocaleString()} label={{ value: formatLabel(yKey), angle: -90, position: 'insideLeft', fill: '#64748b', fontSize: 10 }} />
              <Tooltip content={<CustomTooltip />} />
              <Area type="monotone" dataKey={yKey} stroke="#00d4ff" fill="#00d4ff" fillOpacity={0.3} strokeWidth={3} name={formatLabel(yKey)} onClick={(e) => { const val = e.activeLabel || (e.payload && e.payload[xKey]); if (val) handleDrillDown(xKey, val); }} style={{ cursor: 'pointer' }} />
            </AreaChart>
          );
        }
        if (chartType === 'pie') {
          return (
            <PieChart>
              <Pie data={sortedData} cx="50%" cy="50%" innerRadius={60} outerRadius={100} paddingAngle={5} dataKey={yKey} nameKey={xKey} onClick={(e) => { const val = e && e.payload ? e.payload[xKey] : (e ? (e.name || e[xKey]) : null); if (val) handleDrillDown(xKey, val); }} style={{ cursor: 'pointer' }} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}>
                {sortedData.map((entry, index) => <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />)}
              </Pie>
              <Tooltip content={<CustomTooltip />} />
            </PieChart>
          );
        }
        return (
          <BarChart data={sortedData} margin={{ top: 20, right: 30, left: 20, bottom: 100 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
            <XAxis dataKey={xKey} stroke="#fff" tick={{fill: '#fff'}} fontSize={11} angle={-45} textAnchor="end" interval={0} />
            <YAxis stroke="#fff" tick={{fill: '#fff'}} fontSize={11} tickFormatter={(v) => Number(v).toLocaleString()} label={{ value: formatLabel(yKey), angle: -90, position: 'insideLeft', fill: '#64748b', fontSize: 10 }} />
            <Tooltip content={<CustomTooltip />} />
            <Bar dataKey={yKey} fill="#00d4ff" radius={[6, 6, 0, 0]} name={formatLabel(yKey)} onClick={(e) => { const val = e[xKey] || (e.payload && e.payload[xKey]); if (val) handleDrillDown(xKey, val); }} style={{ cursor: 'pointer' }} />
          </BarChart>
        );
      };
      return (
        <div className="card chart-container interactive">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '15px' }}>
            <h3 style={{ margin: 0 }}>📊 VIEW: {result.chart_type?.toUpperCase()}</h3>
            <div style={{ fontSize: '0.75rem', color: '#64748b' }}>X: {xKey} | Y: {yKey}</div>
          </div>
          <div style={{ width: '100%', overflowX: isScrollable ? 'auto' : 'hidden' }}>
            <div style={{ width: isScrollable ? `${dynamicWidth}px` : '100%', minHeight: '400px' }}>
              <ResponsiveContainer width="100%" height={400}>{renderVisualization()}</ResponsiveContainer>
            </div>
          </div>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="main-content-inner">
      <header className="page-header">
        <h1>Dashboard <span style={{ color: '#64748b', fontWeight: '400' }}>/ AI Intelligence</span></h1>
      </header>
      <div className="breadcrumb-container" style={{ display: 'flex', alignItems: 'center', gap: '15px', marginBottom: '20px' }}>
        {drillDownPath.length > 0 && <button className="back-btn" onClick={goBack}>← BACK</button>}
        <div className="breadcrumb">
          <span onClick={() => drillDownPath.length > 0 && goBack()} style={{ cursor: drillDownPath.length > 0 ? 'pointer' : 'default' }}>Home</span>
          {drillDownPath.map((item, i) => <span key={i}> ❯ {item}</span>)}
        </div>
      </div>
      <div className="card input-card">
        <form onSubmit={handleSubmit} className="query-form">
          <input type="text" value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search your data..." className="query-input" />
          <button type="submit" disabled={loading} className="query-btn">{loading ? '⏳...' : '🚀 RUN'}</button>
        </form>
        <div className="filters-container" style={{ marginTop: '15px', display: 'flex', gap: '20px', flexWrap: 'wrap', alignItems: 'center', borderTop: '1px solid #1e293b', paddingTop: '15px' }}>
          <select value={filters.category} onChange={(e) => setFilters({...filters, category: e.target.value})} style={{ background: '#1e293b', border: '1px solid #334155', color: '#fff', padding: '5px', borderRadius: '4px', fontSize: '0.8rem' }}>
            <option value="all">Category: All</option>
            <option value="Clothing">Clothing</option>
            <option value="Electronics">Electronics</option>
            <option value="Home Decor">Home Decor</option>
          </select>
          <select value={filters.gender} onChange={(e) => setFilters({...filters, gender: e.target.value})} style={{ background: '#1e293b', border: '1px solid #334155', color: '#fff', padding: '5px', borderRadius: '4px', fontSize: '0.8rem' }}>
            <option value="all">Gender: All</option>
            <option value="Male">Male</option>
            <option value="Female">Female</option>
          </select>
          <button onClick={() => setFilters({category: 'all', gender: 'all', startDate: '', endDate: ''})} style={{ fontSize: '0.8rem', color: '#00d4ff', background: 'none', border: 'none', cursor: 'pointer', textDecoration: 'underline' }}>Reset</button>
        </div>
      </div>
      {loading && <ThinkingState step={loading} />}
      {error && <div className="error-alert">❌ Error: {error}</div>}
      {!loading && result && (
        <div className="results-container">
          <div className="card interpretation-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '15px' }}>
              <h3 style={{ margin: 0 }}>🧭 INTERPRETATION</h3>
              <button onClick={() => setShowSql(!showSql)} style={{ fontSize: '0.7rem', background: '#1e293b', color: '#fff', border: '1px solid #334155', padding: '4px 12px', borderRadius: '6px', cursor: 'pointer' }}>{showSql ? 'HIDE SQL' : 'VIEW SQL'}</button>
            </div>
            <div className="badges" style={{ marginBottom: '15px' }}>
              <span><strong>METRIC:</strong> {result.interpretation.metric}</span>
              <span><strong>CHART:</strong> {result.chart_type?.toUpperCase()}</span>
            </div>
            {showSql && <pre style={{ fontSize: '0.8rem', overflowX: 'auto' }}>{result.sql}</pre>}
          </div>
          <div className="dashboard-grid">
            <div className="card table-card" style={{ flex: '1.2' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '15px' }}>
                <h3 style={{ margin: 0 }}>📋 DATA ({sortedData.length} rows)</h3>
                <div style={{ display: 'flex', gap: '10px' }}>
                  <button onClick={() => setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')} style={{ fontSize: '0.75rem', background: '#334155', color: '#fff', border: 'none', padding: '5px 12px', borderRadius: '6px', cursor: 'pointer' }}>SORT {sortOrder === 'asc' ? '↑' : sortOrder === 'desc' ? '↓' : '↕'}</button>
                  <button onClick={() => onExport(sortedData)} style={{ fontSize: '0.75rem', background: '#10b981', color: '#fff', border: 'none', padding: '5px 12px', borderRadius: '6px', cursor: 'pointer' }}>📥 CSV</button>
                </div>
              </div>
              <div className="table-wrapper">
                <table>
                  <thead><tr>{Object.keys(sortedData[0] || {}).map(k => <th key={k}>{k.toUpperCase()}</th>)}</tr></thead>
                  <tbody>{sortedData.map((row, i) => <tr key={i}>{Object.values(row).map((v, j) => <td key={j}>{typeof v === 'number' ? v.toLocaleString() : v}</td>)}</tr>)}</tbody>
                </table>
              </div>
            </div>
            {renderChart()}
          </div>
          <div className="card insights-card">
            <h3>💡 SMART INSIGHTS</h3>
            <div className="insights-text" style={{ whiteSpace: 'pre-wrap' }}>{result.insights}</div>
          </div>

          {result.ml_insights && (
            <div className="ml-insights-container">
              <div className="card ml-card churn">
                <h3 style={{ color: '#ec4899' }}>🔄 CHURN PREDICTION</h3>
                {typeof result.ml_insights.churn_prediction === 'object' ? (
                  <div className="churn-details">
                     <p style={{ fontSize: '1.1rem', fontWeight: 'bold', marginBottom: '8px' }}>
                        {result.ml_insights.churn_prediction.message}
                     </p>
                     {result.ml_insights.churn_prediction.status === 'fallback' && (
                        <div style={{ fontSize: '0.8rem', background: 'rgba(236, 72, 153, 0.1)', padding: '8px', borderRadius: '4px', marginTop: '10px', borderLeft: '3px solid #ec4899' }}>
                           <strong>Reason:</strong> {result.ml_insights.churn_prediction.reason} <br/>
                           <strong>Hint:</strong> {result.ml_insights.churn_prediction.suggestion}
                        </div>
                     )}
                  </div>
                ) : (
                  <p style={{ fontSize: '1.2rem', fontWeight: 'bold' }}>{result.ml_insights.churn_prediction}</p>
                )}
                <div style={{ fontSize: '0.75rem', color: '#64748b', marginTop: '10px' }}>Powered by RandomForestClassifier</div>
              </div>

              <div className="card ml-card recommendations">
                <h3 style={{ color: '#8b5cf6' }}>🎯 RECOMMENDATIONS</h3>
                <ul style={{ padding: 0, margin: 0, listStyle: 'none' }}>
                  {result.ml_insights.recommendations.map((rec, i) => (
                    <li key={i} style={{ fontSize: '1rem', marginBottom: '8px', color: '#cbd5e1' }}>
                      {rec.split('**').map((part, index) => index % 2 === 1 ? <strong key={index} style={{ color: '#fff' }}>{part}</strong> : part)}
                    </li>
                  ))}
                </ul>
              </div>

              <div className="card ml-card anomalies">
                <h3 style={{ color: '#f59e0b' }}>🚩 ANOMALY DETECTION</h3>
                {result.ml_insights.anomalies.length > 0 ? (
                  result.ml_insights.anomalies.map((ano, i) => (
                    <div key={i} style={{ background: 'rgba(245, 158, 11, 0.1)', padding: '12px', borderRadius: '8px', borderLeft: '3px solid #f59e0b', fontSize: '0.95rem' }}>
                      {ano.split('**').map((part, index) => index % 2 === 1 ? <strong key={index} style={{ color: '#fff' }}>{part}</strong> : part)}
                    </div>
                  ))
                ) : (
                  <div style={{ color: '#10b981', display: 'flex', alignItems: 'center', gap: '8px' }}>
                    ✅ <span style={{ fontSize: '1rem' }}>No unusual behavior detected.</span>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

const AnalyticsPage = ({ title, endpoint, onExport }) => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [sortOrder, setSortOrder] = useState('none');

  useEffect(() => {
    setLoading(true);
    axios.get(`http://localhost:5000/analytics/${endpoint}`)
      .then(res => setData(res.data))
      .catch(err => console.error(err))
      .finally(() => setLoading(false));
  }, [endpoint]);

  const getSortedData = (dataToSort) => {
    if (sortOrder === 'none' || !dataToSort || dataToSort.length === 0) return dataToSort;
    const keys = Object.keys(dataToSort[0]);
    const numKey = keys.find(k => typeof dataToSort[0][k] === 'number') || keys[0];
    return [...dataToSort].sort((a, b) => {
      const valA = a[numKey] || 0;
      const valB = b[numKey] || 0;
      return sortOrder === 'asc' ? valA - valB : valB - valA;
    });
  };

  const sortedData = getSortedData(data);

  if (loading) return <ThinkingState step="⚡ SYNCING DATA..." />;
  if (sortedData.length === 0) return <div className="loading">No data found for {title}.</div>;

  const keys = Object.keys(sortedData[0]);
  const numCol = keys.find(k => typeof sortedData[0][k] === 'number') || keys[keys.length - 1];
  const catCol = keys.find(k => k !== numCol) || keys[0];
  const dynamicWidth = Math.max(100, sortedData.length * 40);

  return (
    <div className="main-content-inner">
      <header className="page-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end' }}>
        <div>
          <h1>{title}</h1>
          <p>Full dataset synchronization. Showing all {sortedData.length} categories.</p>
        </div>
        <div style={{ display: 'flex', gap: '10px' }}>
          <button onClick={() => setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')} style={{ marginBottom: '1rem', background: '#334155', color: '#fff', border: 'none', padding: '8px 20px', borderRadius: '8px', cursor: 'pointer', fontWeight: 'bold' }}>
            SORT {sortOrder === 'asc' ? '↑' : sortOrder === 'desc' ? '↓' : '↕'}
          </button>
          <button onClick={() => onExport(sortedData)} style={{ marginBottom: '1rem', background: '#10b981', color: '#fff', border: 'none', padding: '8px 20px', borderRadius: '8px', cursor: 'pointer', fontWeight: 'bold' }}>📥 EXPORT CSV</button>
        </div>
      </header>
      <div className="dashboard-grid full-width">
        <div className="card viz-card full">
          <h3>📊 {numCol.toUpperCase()} DISTRIBUTION</h3>
          <div style={{ width: '100%', overflowX: 'auto' }}>
            <div style={{ width: sortedData.length > 15 ? `${dynamicWidth}px` : '100%', minHeight: '400px' }}>
              <ResponsiveContainer width="100%" height={400}>
                <BarChart data={sortedData} margin={{ top: 20, right: 30, left: 20, bottom: 80 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                  <XAxis dataKey={catCol} stroke="#fff" tick={{fill: '#fff'}} fontSize={10} interval={0} angle={-45} textAnchor="end" />
                  <YAxis stroke="#fff" tick={{fill: '#fff'}} fontSize={11} tickFormatter={(v) => Number(v).toLocaleString()} label={{ value: numCol.toUpperCase(), angle: -90, position: 'insideLeft', fill: '#64748b', fontSize: 10 }} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey={numCol} fill="#00d4ff" radius={[4, 4, 0, 0]} name={numCol.toUpperCase()} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
        <div className="card viz-card">
          <h3>📈 {numCol.toUpperCase()} CONTRIBUTION</h3>
          <ResponsiveContainer width="100%" height={400}>
            <PieChart>
              <Pie data={sortedData} cx="50%" cy="50%" innerRadius={60} outerRadius={100} paddingAngle={5} dataKey={numCol} nameKey={catCol} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}>
                {sortedData.map((entry, index) => <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />)}
              </Pie>
              <Tooltip content={<CustomTooltip />} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>
      <div className="card table-card full">
        <h3>Raw Aggregated Dataset ({sortedData.length} Categories)</h3>
        <div className="table-wrapper">
          <table>
            <thead><tr>{keys.map(k => <th key={k}>{k.toUpperCase()}</th>)}</tr></thead>
            <tbody>{sortedData.map((row, i) => <tr key={i}>{Object.values(row).map((v, j) => <td key={j}>{typeof v === 'number' ? v.toLocaleString() : v}</td>)}</tr>)}</tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

// --- Main App ---

function App() {
  const [currentPage, setCurrentPage] = useState('ai-query');
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(null);
  const [result, setResult] = useState(null);
  const [prevResult, setPrevResult] = useState(null);
  const [error, setError] = useState(null);
  const [drillDownPath, setDrillDownPath] = useState([]);
  const [filters, setFilters] = useState({ category: 'all', gender: 'all', startDate: '', endDate: '' });
  const [toast, setToast] = useState(null);
  const [mapping, setMapping] = useState(null);

  useEffect(() => {
    fetchMapping();
  }, []);

  const fetchMapping = async () => {
    try {
      const res = await axios.get('http://localhost:5000/api/schema-mapping');
      setMapping(res.data);
    } catch (err) {
      console.warn("Failed to fetch schema mapping", err);
    }
  };

  const handleSaveMapping = async (newMapping) => {
    setLoading("🔄 Updating Dataset Overrides...");
    try {
      await axios.post('http://localhost:5000/api/schema-mapping', newMapping);
      setToast("Manual override applied successfully!");
      fetchMapping();
    } catch (err) {
      setToast("Failed to apply override.");
    } finally {
      setLoading(null);
    }
  };


  const handleQuery = async (e, customQuery = null) => {
    if (e) e.preventDefault();
    const targetQuery = customQuery || query;
    if (!targetQuery) return;
    
    // Clear any existing timeouts to prevent race conditions
    if (window.queryTimeouts) {
      window.queryTimeouts.forEach(t => clearTimeout(t));
    }
    window.queryTimeouts = [];

    setLoading("🤖 Understanding your query..."); 
    setResult(null); 
    setError(null);
    setPrevResult(null);
    setDrillDownPath([]);

    window.queryTimeouts.push(setTimeout(() => setLoading("🔍 Generating SQL..."), 800));
    window.queryTimeouts.push(setTimeout(() => setLoading("📊 Fetching insights..."), 1600));

    try {
      const response = await axios.post('http://localhost:5000/query', { 
        query: targetQuery, 
        filters: filters 
      });
      
      // Success: Clear timeouts and stop loading immediately
      window.queryTimeouts.forEach(t => clearTimeout(t));
      setResult(response.data);
      setLoading(null);

    } catch (err) {
      // Error: Clear timeouts and stop loading immediately
      window.queryTimeouts.forEach(t => clearTimeout(t));
      const msg = err.response?.data?.error || err.response?.data?.message || 'Backend connection failed.';
      setError(msg);
      setToast(msg);
      setLoading(null);
    }
  };

  const handleDrillDown = async (field, value) => {
    // 1. Frontend Validation (Hard Termination)
    if (!value || String(value).trim() === "") {
      console.error("Invalid drill-down selection (Empty value).");
      setToast("Invalid selection. Please try again.");
      return;
    }
    
    setLoading(`🔍 Drilling into ${value}...`);
    setPrevResult(result); // Save for 'Back' button
    
    try {
      const response = await axios.post('http://localhost:5000/query', { 
        query: query,
        drill_down: { field, value: String(value).trim() }
      });
      setResult(response.data);
      setDrillDownPath([...drillDownPath, value]);
    } catch (err) {
      const msg = err.response?.data?.message || err.response?.data?.error || "Drill-down failed.";
      setError(msg);
      setToast(msg);
    } finally {
      setLoading(null);
    }
  };

  const goBack = () => {
    if (prevResult) {
      setResult(prevResult);
      setPrevResult(null);
      setDrillDownPath([]);
    }
  };

  const handleFileUpload = async (file) => {
    if (!file) return;
    setLoading("📤 Uploading & Processing Dataset...");
    const formData = new FormData();
    formData.append('file', file);
    try {
      await axios.post('http://localhost:5000/api/upload', formData);
      setToast("Dataset processed successfully!");
      fetchMapping(); // Refresh mapping display after upload
    } catch (err) {
      setError("Failed to upload dataset.");
    } finally {
      setLoading(null);
    }
  };

  const handleExportCSV = async (dataToExport) => {
    if (!dataToExport || dataToExport.length === 0) return;
    try {
      const response = await axios.post('http://localhost:5000/export', { data: dataToExport }, { responseType: 'blob' });
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `dashboard_report_${new Date().toISOString().split('T')[0]}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    } catch (err) {
      console.error("Export failed", err);
      setToast("Failed to export CSV. Please try again.");
    }
  };

  return (
    <div className="layout">
      <Sidebar 
        currentPage={currentPage} 
        setCurrentPage={setCurrentPage} 
        onUpload={handleFileUpload}
        mapping={mapping} 
        onSaveMapping={handleSaveMapping}
      />
      <main className="main-content">
        {currentPage === 'ai-query' && (
          <AIQueryPage 
            query={query} setQuery={setQuery} 
            handleSubmit={handleQuery} 
            handleDrillDown={handleDrillDown}
            goBack={goBack}
            loading={loading} 
            result={result} 
            error={error} 
            drillDownPath={drillDownPath}
            filters={filters}
            setFilters={setFilters}
            onExport={handleExportCSV}
          />

        )}
        {currentPage === 'sales' && <AnalyticsPage title="Sales Intelligence" endpoint="sales" onExport={handleExportCSV} />}
        {currentPage === 'customer' && <AnalyticsPage title="Customer Retention" endpoint="customers" onExport={handleExportCSV} />}
        {currentPage === 'product' && <AnalyticsPage title="Inventory & Logistics" endpoint="products" onExport={handleExportCSV} />}
      </main>
      {toast && <Toast message={toast} onClose={() => setToast(null)} />}
    </div>
  );
}

export default App;
