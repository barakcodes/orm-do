export const adminHtml = `<!DOCTYPE html>
<html lang="en">
<head>
 <meta charset="UTF-8">
 <meta name="viewport" content="width=device-width, initial-scale=1.0">
 <title>User Management Admin</title>
 <link href="https://cdnjs.cloudflare.com/ajax/libs/tailwindcss/2.2.19/tailwind.min.css" rel="stylesheet">
 <script src="https://cdnjs.cloudflare.com/ajax/libs/alpinejs/3.10.3/cdn.min.js" defer></script>
</head>
<body class="bg-gray-100 min-h-screen">
 <div x-data="adminApp()" x-init="fetchUsers()" class="container mx-auto px-4 py-8">
 <header class="mb-8">
 <h1 class="text-3xl font-bold text-gray-800">User Management Admin</h1>
 </header>
 
 <!-- Outerbase Connection Card -->
 <div class="bg-white rounded-lg shadow-md p-6 mb-8">
   <h2 class="text-xl font-semibold mb-4">Connect Database</h2>
   <div class="mb-4">
     <p class="mb-2"><span class="font-medium">URL:</span> https://ormdo.wilmake.com/api/db</p>
     <p class="mb-2"><span class="font-medium">Type:</span> internal</p>
     <p class="mb-2"><span class="font-medium">Access-key:</span> my-secret-key</p>
   </div>
   <a href="https://studio.outerbase.com/local/new-base/starbase?url=https://ormdo.wilmake.com/api/db&type=internal&access-key=my-secret-key" 
      class="inline-block px-4 py-2 text-sm font-medium text-white bg-purple-600 rounded-md hover:bg-purple-700 focus:outline-none focus:ring-2 focus:ring-purple-500"
      target="_blank"
      rel="noopener noreferrer">
     Connect with Outerbase
   </a>
 </div>
 
 <!-- Form for adding/editing users -->
 <div class="bg-white rounded-lg shadow-md p-6 mb-8">
 <h2 class="text-xl font-semibold mb-4" x-text="editingUser.id ? 'Edit User' : 'Add New User'"></h2>
 <form @submit.prevent="saveUser()">
 <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
 <div>
 <label class="block text-sm font-medium text-gray-700 mb-1" for="name">Name</label>
 <input
 type="text"
 id="name"
 x-model="editingUser.name"
 class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
 required
 >
 </div>
 <div>
 <label class="block text-sm font-medium text-gray-700 mb-1" for="email">Email</label>
 <input
 type="email"
 id="email"
 x-model="editingUser.email"
 class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
 required
 >
 </div>
 </div>
 <div class="flex justify-end space-x-2">
 <button
 type="button"
 x-show="editingUser.id"
 @click="resetForm()"
 class="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-200 rounded-md hover:bg-gray-300 focus:outline-none focus:ring-2 focus:ring-gray-500"
 >
 Cancel
 </button>
 <button
 type="submit"
 class="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
 >
 Save
 </button>
 </div>
 </form>
 </div>
 <!-- Status message -->
 <div
 x-show="statusMessage"
 x-transition
 class="mb-6 p-4 rounded-md"
 :class="statusSuccess ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'"
 >
 <p x-text="statusMessage"></p>
 </div>
 <!-- User list -->
 <div class="bg-white rounded-lg shadow-md overflow-hidden">
 <h2 class="text-xl font-semibold p-6 border-b">Users</h2>
 <div x-show="loading" class="flex justify-center items-center p-8">
 <svg class="animate-spin h-8 w-8 text-gray-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
 <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
 <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
 </svg>
 </div>
 <div x-show="!loading && users.length === 0" class="p-8 text-center text-gray-500">
 No users found. Add your first user above.
 </div>
 <div x-show="!loading && users.length > 0" class="overflow-x-auto">
 <table class="w-full">
 <thead class="bg-gray-50">
 <tr>
 <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
 <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Email</th>
 <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Created At</th>
 <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
 </tr>
 </thead>
 <tbody class="bg-white divide-y divide-gray-200">
 <template x-for="user in users" :key="user.id">
 <tr>
 <td class="px-6 py-4 whitespace-nowrap">
 <div class="text-sm font-medium text-gray-900" x-text="user.name"></div>
 </td>
 <td class="px-6 py-4 whitespace-nowrap">
 <div class="text-sm text-gray-500" x-text="user.email"></div>
 </td>
 <td class="px-6 py-4 whitespace-nowrap">
 <div class="text-sm text-gray-500" x-text="formatDate(user.created_at)"></div>
 </td>
 <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
 <button
 @click="editUser(user)"
 class="text-blue-600 hover:text-blue-900 mr-3"
 >
 Edit
 </button>
 <button
 @click="confirmDeleteUser(user)"
 class="text-red-600 hover:text-red-900"
 >
 Delete
 </button>
 </td>
 </tr>
 </template>
 </tbody>
 </table>
 </div>
 </div>
 <!-- Delete confirmation modal -->
 <div
 x-show="showDeleteModal"
 x-transition
 class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50"
 >
 <div class="bg-white rounded-lg shadow-xl max-w-md w-full p-6" @click.away="showDeleteModal = false">
 <h3 class="text-lg font-semibold mb-4">Confirm Deletion</h3>
 <p class="mb-6">Are you sure you want to delete <span class="font-semibold" x-text="userToDelete?.name"></span>? This action cannot be undone.</p>
 <div class="flex justify-end space-x-2">
 <button
 @click="showDeleteModal = false"
 class="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-200 rounded-md hover:bg-gray-300 focus:outline-none focus:ring-2 focus:ring-gray-500"
 >
 Cancel
 </button>
 <button
 @click="deleteUser()"
 class="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-md hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-500"
 >
 Delete
 </button>
 </div>
 </div>
 </div>
 </div>
 <script>
 function adminApp() {
 return {
 users: [],
 loading: true,
 editingUser: {
 id: null,
 name: '',
 email: ''
 },
 userToDelete: null,
 showDeleteModal: false,
 statusMessage: '',
 statusSuccess: true,
 // Fetch all users
 async fetchUsers() {
 this.loading = true;
 try {
 const response = await fetch('/api/users');
 if (!response.ok) {
 throw new Error('Failed to fetch users');
 }
 this.users = await response.json();
 } catch (error) {
 this.showStatus('Error loading users: ' + error.message, false);
 } finally {
 this.loading = false;
 }
 },
 // Save user (create or update)
 async saveUser() {
 try {
 let response;
 if (this.editingUser.id) {
 // Update existing user
 response = await fetch(\`/api/users/\${this.editingUser.id}\`, {
 method: 'PUT',
 headers: {
 'Content-Type': 'application/json'
 },
 body: JSON.stringify({
 name: this.editingUser.name,
 email: this.editingUser.email
 })
 });
 } else {
 // Create new user
 response = await fetch('/api/users', {
 method: 'POST',
 headers: {
 'Content-Type': 'application/json'
 },
 body: JSON.stringify({
 name: this.editingUser.name,
 email: this.editingUser.email
 })
 });
 }
 if (!response.ok) {
 const errorData = await response.json();
 throw new Error(errorData.error || 'Failed to save user');
 }
 const savedUser = await response.json();
 this.showStatus(\`User \${this.editingUser.id ? 'updated' : 'created'} successfully!\`, true);
 this.resetForm();
 this.fetchUsers();
 } catch (error) {
 this.showStatus('Error: ' + error.message, false);
 }
 },
 // Edit a user
 editUser(user) {
 this.editingUser = {
 id: user.id,
 name: user.name,
 email: user.email
 };
 window.scrollTo({ top: 0, behavior: 'smooth' });
 },
 // Confirm delete
 confirmDeleteUser(user) {
 this.userToDelete = user;
 this.showDeleteModal = true;
 },
 // Delete a user
 async deleteUser() {
 if (!this.userToDelete) return;
 try {
 const response = await fetch(\`/api/users/\${this.userToDelete.id}\`, {
 method: 'DELETE'
 });
 if (!response.ok) {
 const errorData = await response.json();
 throw new Error(errorData.error || 'Failed to delete user');
 }
 this.showStatus(\`User \${this.userToDelete.name} deleted successfully!\`, true);
 this.showDeleteModal = false;
 this.userToDelete = null;
 this.fetchUsers();
 } catch (error) {
 this.showStatus('Error: ' + error.message, false);
 this.showDeleteModal = false;
 }
 },
 // Reset the form
 resetForm() {
 this.editingUser = {
 id: null,
 name: '',
 email: ''
 };
 },
 // Show status message
 showStatus(message, success) {
 this.statusMessage = message;
 this.statusSuccess = success;
 // Clear message after 5 seconds
 setTimeout(() => {
 this.statusMessage = '';
 }, 5000);
 },
 // Format date for display
 formatDate(dateString) {
 if (!dateString) return '';
 const date = new Date(dateString);
 return new Intl.DateTimeFormat('en-US', {
 year: 'numeric',
 month: 'short',
 day: 'numeric',
 hour: '2-digit',
 minute: '2-digit'
 }).format(date);
 }
 };
 }
 </script>
</body>
</html>`;
