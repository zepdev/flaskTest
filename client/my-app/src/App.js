import React, { useState, useEffect } from 'react';
import logo from './logo.svg';
import './App.css';

function App() {

  const [currentTime, setCurrentTime] = useState(0);
  const [msg, setMsg] = useState(0);

  useEffect(() => {
    fetch('/time').then(res => res.json()).then(data => {
      setCurrentTime(data.time);
    });
  }, []);


  // useEffect(() => {
  //   fetch('http://localhost:8000/dummy/forecast').then(res => res.json()).then(data => {
  //     console.log(data);
  //     setMsg(data);
  //   });
  // }, []);

  const handleClick = () => {
    fetch("http://localhost:8000/dummy/forecast", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        weight: 2,
      }),
    })
      .then((res) => res.json())
      .then((data) => {
        setMsg(data);
      });
  };
console.log(msg)
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.js</code> and save to reload.
        </p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React Now.
        </a>
        <p style={{ marginBottom: 15 }}>The current time is {currentTime}.</p>

        {msg && (
        <table>
          {msg.map((row, idx) => (
            <>
              {Object.keys(row).map((elem) => (
                <tr>{row[elem]}</tr>
              ))}
            </>
          ))}
        </table>
      )}
                <button onClick={handleClick}>Add</button>
              </header>
    </div>
        );
}

        export default App;
