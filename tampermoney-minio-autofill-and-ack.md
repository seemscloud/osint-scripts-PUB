```js
// ==UserScript==
// @name         MinIO Auto Login + Ack (react-safe)
// @namespace    http://tampermonkey.net/
// @version      2025-12-26
// @match        *://*/*
// @grant        none
// ==/UserScript==

(function () {
    if (!document.title || !document.title.toLowerCase().includes("minio")) return

    const USERNAME = "minioadmin"
    const PASSWORD = "minioadmin"

    const setNativeValue = (el, value) => {
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value").set
        setter.call(el, value)
        el.dispatchEvent(new Event("input", { bubbles: true }))
        el.dispatchEvent(new Event("change", { bubbles: true }))
    }

    setInterval(() => {
        const userInput = document.querySelector("#accessKey")
        const passInput = document.querySelector("#secretKey")
        const loginBtn = document.querySelector("#do-login")
        const ackBtn = document.querySelector("#acknowledge-confirm")

        if (userInput && userInput.value !== USERNAME) {
            userInput.focus()
            setNativeValue(userInput, USERNAME)
            userInput.blur()
        }

        if (passInput && passInput.value !== PASSWORD) {
            passInput.focus()
            setNativeValue(passInput, PASSWORD)
            passInput.blur()
        }

        if (loginBtn && !loginBtn.disabled) {
            loginBtn.click()
        }

        if (ackBtn) {
            ackBtn.click()
        }
    }, 100)
})()
```
