module.exports = function (grunt) {
    var theme = grunt.option('theme') || 'myProject';

     grunt.initConfig({
           less: {
                options: {
                     compress : false,
                     yuicompress : false,
                     optimization : 2
                },
                compileTheme: {
                    options: {
                        strictMath: false,
                        sourceMap: false
                    },
                    src: 'themes/' + theme + '/package.less',
                    dest: 'themes/' + theme + '/app.css'
                },
           },
           watch: {
                configFiles: {
                     files : ['Gruntfile.js'],
                     tasks : ['less'],
                     options : {
                           reload : true
                     }
                },
                
           
                styles : {
                     files : ['themes/**/*.less', 'lib/**/*.less', 'components/**/*.less'],
                     tasks : ['less'],
                     options : {
                           nospawn : true
                     }
                },   
           }          
     });
     grunt.loadNpmTasks('grunt-contrib-less');
     grunt.loadNpmTasks('grunt-contrib-watch');
     grunt.registerTask('default', ['watch']);
};
