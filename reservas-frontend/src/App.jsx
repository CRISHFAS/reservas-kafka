import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import ShowSelectorPage from './pages/ShowSelectorPage'
import SeatMapPage from './pages/SeatMapPage'
import HowItWorksPage from './pages/HowItWorksPage'

function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<ShowSelectorPage />} />
          <Route path="/show/:showId" element={<SeatMapPage />} />
          <Route path="/como-funciona" element={<HowItWorksPage />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  )
}

export default App
