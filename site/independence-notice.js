class IndependenceNotice extends HTMLElement {
  connectedCallback() {
    const root = this.attachShadow({ mode: "open" });
    root.innerHTML = `
      <style>
        :host {
          display: block;
          width: 100%;
          box-sizing: border-box;
          padding: 10px 14px;
          border: 1px solid #e2c567;
          border-radius: 6px;
          background: #fff7d6;
          color: #493b12;
          font: 13px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        }
        a {
          color: #705700;
          font-weight: 600;
        }
      </style>
      <strong>Independent website.</strong>
      This is not an official AC Transit website and is not affiliated with or endorsed by AC Transit.
      <a href="https://www.actransit.org/">Visit the official AC Transit website.</a>
    `;
  }
}

customElements.define("independence-notice", IndependenceNotice);
document.body.prepend(document.createElement("independence-notice"));
