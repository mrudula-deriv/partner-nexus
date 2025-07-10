import React from 'react';

interface ProgressBarProps {
  progress: number;
  message: string;
}

const ProgressBar: React.FC<ProgressBarProps> = ({ progress, message }) => {
  return (
    <div style={{
      marginTop: '1.5rem',
      marginBottom: '1.5rem',
      width: '100%',
      padding: '1rem',
      backgroundColor: 'var(--bg-tertiary)',
      borderRadius: 'var(--radius-md)',
      border: '1px solid var(--border-color)'
    }}>
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '0.75rem'
      }}>
        <span style={{
          fontSize: '0.9rem',
          color: 'var(--text-primary)',
          fontWeight: '500'
        }}>
          {message}
        </span>
        <span style={{
          fontSize: '0.9rem',
          color: 'var(--primary-red)',
          fontWeight: '600'
        }}>
          {progress}%
        </span>
      </div>
      <div style={{
        width: '100%',
        height: '8px',
        backgroundColor: '#e5e7eb',
        borderRadius: '4px',
        overflow: 'hidden',
        boxShadow: 'inset 0 1px 2px rgba(0, 0, 0, 0.1)'
      }}>
        <div
          style={{
            width: `${progress}%`,
            height: '100%',
            backgroundColor: 'var(--primary-red)',
            borderRadius: '4px',
            transition: 'width 0.3s ease-in-out',
            boxShadow: '0 1px 2px rgba(0, 0, 0, 0.1)'
          }}
        />
      </div>
    </div>
  );
};

export default ProgressBar; 