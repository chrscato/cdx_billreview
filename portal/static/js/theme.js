/**
 * Theme management for Bill Review Portal
 * Safely integrates with Bootstrap 3 and existing functionality
 */
(function() {
    'use strict';

    // Theme configuration
    const ThemeManager = {
        // Theme state
        isDarkMode: false,
        
        // DOM elements
        elements: {
            body: document.body,
            themeToggle: null,
            themeIcon: null
        },
        
        // Initialize theme manager
        init: function() {
            // Wait for DOM to be fully loaded
            if (document.readyState === 'loading') {
                document.addEventListener('DOMContentLoaded', this.setup.bind(this));
            } else {
                this.setup();
            }
        },
        
        // Setup theme manager
        setup: function() {
            // Get DOM elements
            this.elements.themeToggle = document.getElementById('theme-toggle');
            this.elements.themeIcon = document.getElementById('theme-icon');
            
            // Check for saved preference
            const savedTheme = localStorage.getItem('theme');
            
            // Check system preference if no saved preference
            if (!savedTheme) {
                const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
                this.isDarkMode = prefersDark;
            } else {
                this.isDarkMode = savedTheme === 'dark';
            }
            
            // Apply initial theme
            this.applyTheme();
            
            // Add event listeners
            this.addEventListeners();
        },
        
        // Add event listeners
        addEventListeners: function() {
            // Theme toggle click handler
            if (this.elements.themeToggle) {
                this.elements.themeToggle.addEventListener('click', (e) => {
                    e.preventDefault();
                    this.toggleTheme();
                });
            }
            
            // System preference change handler
            const colorSchemeQuery = window.matchMedia('(prefers-color-scheme: dark)');
            colorSchemeQuery.addListener((e) => {
                if (!localStorage.getItem('theme')) {
                    this.isDarkMode = e.matches;
                    this.applyTheme();
                }
            });
        },
        
        // Toggle theme
        toggleTheme: function() {
            this.isDarkMode = !this.isDarkMode;
            this.applyTheme();
            localStorage.setItem('theme', this.isDarkMode ? 'dark' : 'light');
        },
        
        // Apply current theme
        applyTheme: function() {
            if (this.isDarkMode) {
                this.elements.body.classList.add('dark-mode');
                if (this.elements.themeIcon) {
                    this.elements.themeIcon.classList.remove('fa-sun');
                    this.elements.themeIcon.classList.add('fa-moon');
                }
            } else {
                this.elements.body.classList.remove('dark-mode');
                if (this.elements.themeIcon) {
                    this.elements.themeIcon.classList.remove('fa-moon');
                    this.elements.themeIcon.classList.add('fa-sun');
                }
            }
        }
    };

    // Initialize theme manager after Bootstrap is loaded
    window.addEventListener('load', function() {
        ThemeManager.init();
    });
})(); 