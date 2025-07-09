import React from 'react';

interface SampleQuestionsProps {
  onQuestionSelect: (question: string) => void;
}

const SampleQuestions: React.FC<SampleQuestionsProps> = ({ onQuestionSelect }) => {
  const sampleQuestions = [
    {
      id: 1,
      label: "Compare Activations",
      question: "Compare Nigeria and India and Vietnam partner activations for March 2025?"
    },
    {
      id: 2,
      label: "February Applications",
      question: "What is the number of partners applications from Feb 2025?"
    },
    {
      id: 3,
      label: "Top 5 Growth Countries",
      question: "What are the 5 countries that experienced the highest % spike in partner activations from April to May 2025?"
    }
  ];

  return (
    <div style={{
      marginBottom: '1.5rem',
      padding: '1rem',
      background: 'var(--background-secondary)',
      borderRadius: '8px',
      border: '1px solid var(--border-color)'
    }}>
      <div style={{ marginBottom: '0.75rem' }}>
        <span style={{ 
          fontSize: '0.875rem', 
          color: 'var(--text-primary)',
          fontWeight: 600 
        }}>
          Try these sample questions:
        </span>
      </div>
      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: '0.75rem'
      }}>
        {sampleQuestions.map((q) => (
          <button
            key={q.id}
            onClick={() => onQuestionSelect(q.question)}
            style={{
              padding: '0.75rem 1.25rem',
              border: '1px solid var(--primary-red)',
              borderRadius: '6px',
              background: 'white',
              color: 'var(--primary-red)',
              cursor: 'pointer',
              fontSize: '0.875rem',
              fontWeight: 500,
              transition: 'all 0.2s ease',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              boxShadow: '0 1px 2px rgba(0, 0, 0, 0.05)'
            }}
            onMouseOver={(e) => {
              e.currentTarget.style.background = 'var(--primary-red-lighter)';
              e.currentTarget.style.borderColor = 'var(--primary-red)';
            }}
            onMouseOut={(e) => {
              e.currentTarget.style.background = 'white';
              e.currentTarget.style.borderColor = 'var(--primary-red)';
            }}
          >
            {q.label}
          </button>
        ))}
      </div>
    </div>
  );
};

export default SampleQuestions; 