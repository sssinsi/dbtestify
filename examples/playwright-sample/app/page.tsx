'use client';

import { useState, useEffect } from 'react';

export default function HomePage() {
  const [count, setCount] = useState<number>(0);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchCounter = async () => {
      try {
        const res = await fetch('/api/counter');
        if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`);
        }
        const data = await res.json();
        setCount(data.value);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchCounter();
  }, []);

  const handleIncrement = async () => {
    try {
      const res = await fetch('/api/counter', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ incrementBy: 1 }),
      });

      if (!res.ok) {
        throw new Error(`HTTP error! status: ${res.status}`);
      }

      const data = await res.json();
      setCount(data.value);
    } catch (err: any) {
      setError(err.message);
    }
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center h-screen bg-gray-100">
        <p className="text-xl text-gray-700">Loading counter...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex justify-center items-center h-screen bg-red-100 text-red-700 p-4 rounded-lg">
        <p className="text-xl">Error: {error}</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100 p-4">
      <h1 className="text-4xl font-bold text-gray-800 mb-6">
        Counter Sample for dbtestify sample
      </h1>
      <p className="text-6xl font-extrabold text-blue-600 mb-8 select-none">
        <label>Count: <output>{count}</output></label>
      </p>
      <button
        onClick={handleIncrement}
        className="px-8 py-4 text-2xl font-semibold text-white bg-blue-500 rounded-lg shadow-lg
                   hover:bg-blue-600 focus:outline-none focus:ring-4 focus:ring-blue-300
                   transition duration-300 ease-in-out transform hover:scale-105"
      >
        Count
      </button>
    </div>
  );
}