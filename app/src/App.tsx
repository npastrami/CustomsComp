/** @jsxImportSource @emotion/react */

import { Container, Typography, Box } from '@mui/material';
import ClientDataTable from './components/ClientDataTable/ClientDataTable';
import { JobInput } from './components/JobInput';
import { JobProvider } from './components/JobInput/JobContext';
import { FileUpload } from './components/FileUpload';

function App() {
  /** Axcess version will be user input for now. versionID will be removed from here when we can get it from Axcess */

  const krGreen = '#1338BE';

  return (
    <JobProvider>
      <Container>
        <Typography
          variant="h1"
          css={{
            color: krGreen,
            textAlign: 'center',
          }}
        >
          Customs Computer
        </Typography>
        
        <JobInput />

        <Box mb={4}>
          <FileUpload />
        </Box>

        <ClientDataTable />
      </Container>
    </JobProvider>
  );
}

export default App;
