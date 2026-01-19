import unittest
from unittest.mock import MagicMock, patch
from beta9 import inference
from beta9.inference import InferenceOptions, ChatMessage, EmbeddingResult

class TestCubicFixes(unittest.TestCase):
    
    @patch('beta9.inference.httpx.Client')
    def test_generate_forwarding_options(self, mock_client_cls):
        """Test that generate() forwards all InferenceOptions correctly."""
        # Setup mock
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_response = MagicMock()
        mock_response.json.return_value = {"response": "test", "done_reason": "stop"}
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response

        # Configure client
        inference.configure()

        # Call generate with all options
        options = InferenceOptions(
            temperature=0.5,
            max_tokens=100,
            stream=False, # generate doesn't support stream=True in SDK yet but param exists
            top_p=0.9,
            frequency_penalty=0.5,
            presence_penalty=0.3
        )
        inference.generate("model", "prompt", options=options)

        # Verify payload
        call_args = mock_client.post.call_args
        self.assertIsNotNone(call_args)
        payload = call_args[1]['json']
        
        # Check all options are present in payload
        self.assertIn('options', payload)
        opts = payload['options']
        self.assertEqual(opts.get('temperature'), 0.5)
        self.assertEqual(opts.get('top_p'), 0.9)
        self.assertEqual(opts.get('num_predict'), 100)
        self.assertEqual(opts.get('frequency_penalty'), 0.5)
        self.assertEqual(opts.get('presence_penalty'), 0.3)

    @patch('beta9.inference.httpx.Client')
    def test_batch_embeddings(self, mock_client_cls):
        """Test that embed() returns all embeddings for batch input."""
        # Setup mock
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_response = MagicMock()
        
        # Mock older Ollama format (single embedding)
        mock_response.json.return_value = {"embedding": [0.1, 0.2, 0.3]}
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response
        
        inference.configure()
        result = inference.embed("model", "single input")
        self.assertEqual(len(result.embedding), 3) # Vector dimension is 3
        self.assertIsInstance(result.embedding[0], float)

        # Mock newer Ollama format (batch embeddings)
        mock_response.json.return_value = {
            "embeddings": [[0.1, 0.1], [0.2, 0.2], [0.3, 0.3]]
        }
        
        result = inference.embed("model", ["a", "b", "c"])
        
        # CURRENT BUG: returns only first embedding [0.1, 0.1]
        # FIX EXPECTATION: result.embeddings should be list of lists
        
        # If the fix works, this should pass
        self.assertEqual(len(result.embeddings), 3) 
        self.assertIsInstance(result.embeddings[0], list)
        
        # Check specific values
        self.assertEqual(result.embeddings[0], [0.1, 0.1])
        self.assertEqual(result.embeddings[2], [0.3, 0.3])

    @patch('beta9.inference.httpx.Client')
    def test_configure_closes_client(self, mock_client_cls):
        """Test that configure() closes the previous client."""
        # Create first mock
        mock_client1 = MagicMock()
        mock_client_cls.return_value = mock_client1
        
        inference.configure()
        
        # Create second mock
        mock_client2 = MagicMock()
        mock_client_cls.return_value = mock_client2
        
        inference.configure()
        
        # Verify first client was closed
        mock_client1.close.assert_called_once()

    @patch('beta9.inference.httpx.Client')
    def test_authentication(self, mock_client_cls):
        """Test that auth token is forwarded in Authorization header."""
        # Setup mock
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"models": []}
        mock_client.get.return_value = mock_response

        # Test 1: With token
        inference.configure(token="secret-token")
        
        # Verify client initialized with headers
        _, kwargs = mock_client_cls.call_args
        headers = kwargs.get('headers', {})
        self.assertEqual(headers.get('Authorization'), 'Bearer secret-token')
        
        # Test 2: Without token
        inference.configure(token=None)
        
        # Verify client initialized without headers
        _, kwargs = mock_client_cls.call_args
        headers = kwargs.get('headers', {})
        self.assertNotIn('Authorization', headers)

if __name__ == '__main__':
    unittest.main()
