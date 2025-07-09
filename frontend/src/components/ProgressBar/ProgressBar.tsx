import React from 'react';

interface ProgressBarProps {
  progress: number;
  message: string;
}

const ProgressBar: React.FC<ProgressBarProps> = ({ progress, message }) => {
  return (
    <div style={{
      marginTop: '1rem',
      marginBottom: '1rem',
      width: '100%'
    }}>
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '0.5rem'
      }}>
        <span style={{
          fontSize: '0.875rem',
          color: 'var(--text-secondary)'
        }}>
          {message}
        </span>
        <span style={{
          fontSize: '0.875rem',
          color: 'var(--text-secondary)',
          fontWeight: 'bold'
        }}>
          {progress}%
        </span>
      </div>
      <div style={{
        width: '100%',
        height: '6px',
        backgroundColor: 'var(--bg-tertiary)',
        borderRadius: '3px',
        overflow: 'hidden'
      }}>
        <div
          style={{
            width: `${progress}%`,
            height: '100%',
            backgroundColor: 'var(--primary-red)',
            borderRadius: '3px',
            transition: 'width 0.3s ease-in-out'
          }}
        />
      </div>
    </div>
  );
};

export default ProgressBar; 