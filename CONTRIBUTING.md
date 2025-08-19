## Contribute to Beta9 📡

# Local Installation

You can run Beta9 locally, or in an existing Kubernetes cluster using our [Helm chart](https://github.com/beam-cloud/beta9/tree/main/deploy/charts/beta9).

### Setting Up the Server

k3d is used for local development. You'll need Docker to get started.

To use our fully automated setup, run the `setup` make target.

```bash
make setup
```

> **Note:** If you encounter permission errors during setup, you may need to fix ownership of the `/usr/local/bin` directory. You can do this by running:
> ```bash
> sudo chown $(whoami) /usr/local/bin
> ```


#### Local DNS

This is required to use an external file service for mulitpart uploads and range downloads. Its optional for using the subdomain middlware (host-based URLs).

```shell
brew install dnsmasq
echo 'address=/cluster.local/127.0.0.1' >> /opt/homebrew/etc/dnsmasq.conf
sudo bash -c 'mkdir -p /etc/resolver'
sudo bash -c 'echo "nameserver 127.0.0.1" > /etc/resolver/cluster.local'
sudo brew services start dnsmasq
```

To use subdomain or host-based URLs, add this to the config and rebuild the Beta9 gateway.

```yaml
gateway:
  invokeURLType: host
```

You should now be able to access your local k3s instance via a domain.

```shell
curl http://beta9-gateway.beta9.svc.cluster.local:1994/api/v1/health
```

### Setting Up the SDK

The SDK is written in Python. You'll need Python 3.8 or higher. Use the `setup-sdk` make target to get started.

```bash
make setup-sdk
```

### Using the SDK

After you've setup the server and SDK, check out the SDK readme [here](sdk/README.md).

## 🔧 Opening an Issue

All issues are managed on Github. Feel free to open a new issue using the templates below:

- [Bug Report](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=bug-report.md&title=)
- [Feature Request](https://github.com/beam-cloud/beta9/issues/new?assignees=&labels=&projects=&template=feature-request.md&title=)

## 📖 Documentation

Improvements to our documentation are extremely valuable. Any PR that improves the documentation -- no matter how small -- is encouraged.

## Tutorials

Are you interested in adding new hands-on examples or tutorials for Beta9? We encourage you to make a PR with your app examples! 

Tutorials are one of the most requested features from users, and examples that demonstrate practical applications of Beta9 are very valuable for the community.

## License

By contributing to the project, you agree that your contributions will be licensed under the AGPL v3 License.
